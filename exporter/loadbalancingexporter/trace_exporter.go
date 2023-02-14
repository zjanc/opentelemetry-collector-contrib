// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package loadbalancingexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/loadbalancingexporter"

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var _ exporter.Traces = (*traceExporterImp)(nil)

type traceExporterImp struct {
	loadBalancer loadBalancer
	resourceKeys []string

	traceConsumer traceConsumer
	stopped       bool
	shutdownWg    sync.WaitGroup
	logger        *zap.Logger
}

type routingEntry struct {
	routingKey routingKey
	keyValue   string
	trace      ptrace.Traces
}

type traceConsumer func(ctx context.Context, td ptrace.Traces) error

// Create new traces exporter
func newTracesExporter(params exporter.CreateSettings, cfg component.Config, logger *zap.Logger) (*traceExporterImp, error) {
	exporterFactory := otlpexporter.NewFactory()

	lb, err := newLoadBalancer(params, cfg, func(ctx context.Context, endpoint string) (component.Component, error) {
		oCfg := buildExporterConfig(cfg.(*Config), endpoint)
		return exporterFactory.CreateTracesExporter(ctx, params, &oCfg)
	})
	if err != nil {
		return nil, err
	}

	traceExporter := traceExporterImp{loadBalancer: lb, logger: logger}

	switch cfg.(*Config).RoutingKey {
	case "service":
		traceExporter.traceConsumer = traceExporter.consumeTracesByResource
		traceExporter.resourceKeys = []string{"service.name"}
	case "resource":
		traceExporter.traceConsumer = traceExporter.consumeTracesByResource
		traceExporter.resourceKeys = cfg.(*Config).ResourceKeys
	case "traceID", "":
		traceExporter.traceConsumer = traceExporter.consumeTracesById
	default:
		return nil, fmt.Errorf("unsupported routing_key: %s", cfg.(*Config).RoutingKey)
	}
	return &traceExporter, nil
}

func buildExporterConfig(cfg *Config, endpoint string) otlpexporter.Config {
	oCfg := cfg.Protocol.OTLP
	oCfg.Endpoint = endpoint
	return oCfg
}

func (e *traceExporterImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *traceExporterImp) Start(ctx context.Context, host component.Host) error {
	return e.loadBalancer.Start(ctx, host)
}

func (e *traceExporterImp) Shutdown(context.Context) error {
	e.stopped = true
	e.shutdownWg.Wait()
	return nil
}

func (e *traceExporterImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return e.traceConsumer(ctx, td)
}

func (e *traceExporterImp) consumeTracesById(ctx context.Context, td ptrace.Traces) error {
	var errs error
	batches := batchpersignal.SplitTraces(td)

	var re []routingEntry
	for _, t := range batches {
		// We convert this batch into a list of routeEntries
		// so it can be batched by batchByEndpoint
		if tid, err := routeByTraceId(t); err == nil {
			re = append(re, routingEntry{
				routingKey: traceIDRouting,
				keyValue:   tid,
				trace:      t,
			})
		} else {
			return err
		}
	}
	for _, b := range e.batchByEndpoint(re) {
		errs = multierr.Append(errs, e.consumeTrace(ctx, b.trace, b.keyValue))
	}
	return errs
}

func (e *traceExporterImp) consumeTracesByResource(ctx context.Context, td ptrace.Traces) error {
	var errs error
	routeBatches, err := splitTracesByResourceAttr(td, e.resourceKeys)
	if err != nil {
		return err
	}
	for routing, batches := range routeBatches {
		switch routing {
		case resourceAttrRouting:
			for _, b := range e.batchByEndpoint(batches) {
				errs = multierr.Append(errs, e.consumeTrace(ctx, b.trace, b.keyValue))
			}
		case traceIDRouting:
			// Batch together these traces and reingest them via the trace id load balancing pipeline
			t := ptrace.NewTraces()
			for _, b := range batches {
				b.trace.ResourceSpans().MoveAndAppendTo(t.ResourceSpans())
			}
			errs = multierr.Append(errs, e.consumeTracesById(ctx, t))
		}
	}
	return errs
}

func (e *traceExporterImp) batchByEndpoint(re []routingEntry) []routingEntry {
	traceMap := make(map[string]routingEntry)
	for _, entry := range re {
		endpoint := e.loadBalancer.Endpoint([]byte(entry.keyValue))
		if _, ok := traceMap[endpoint]; !ok {
			traceMap[endpoint] = routingEntry{
				routingKey: entry.routingKey,
				// We only need to store one keyValue since we expect all keyValues
				// grouped here to hash to the same endpoint
				keyValue: entry.keyValue,
				trace:    ptrace.NewTraces(),
			}
		}
		entry.trace.ResourceSpans().MoveAndAppendTo(traceMap[endpoint].trace.ResourceSpans())
	}
	var res []routingEntry
	for _, tm := range traceMap {
		res = append(res, tm)
	}
	return res
}

func (e *traceExporterImp) consumeTrace(ctx context.Context, td ptrace.Traces, rid string) error {
	// Routes a single trace via a given routing ID
	endpoint := e.loadBalancer.Endpoint([]byte(rid))
	exp, err := e.loadBalancer.Exporter(endpoint)
	if err != nil {
		return err
	}

	te, ok := exp.(exporter.Traces)
	if !ok {
		return fmt.Errorf("unable to export traces, unexpected exporter type: expected exporter.Traces but got %T", exp)
	}

	start := time.Now()
	err = te.ConsumeTraces(ctx, td)
	duration := time.Since(start)

	if err == nil {
		_ = stats.RecordWithTags(
			ctx,
			[]tag.Mutator{tag.Upsert(endpointTagKey, endpoint), successTrueMutator},
			mBackendLatency.M(duration.Milliseconds()))
	} else {
		_ = stats.RecordWithTags(
			ctx,
			[]tag.Mutator{tag.Upsert(endpointTagKey, endpoint), successFalseMutator},
			mBackendLatency.M(duration.Milliseconds()))
	}
	return err
}

func getResourceAttrValue(rs ptrace.ResourceSpans, resourceKeys []string) (string, bool) {
	res := ""
	found := false
	rs.Resource().Attributes().Range(
		func(k string, v pcommon.Value) bool {
			for _, attrKey := range resourceKeys {
				if k == attrKey {
					res = v.Str()
					found = true
					return false
				}
			}
			return true
		})
	return res, found
}

func splitTracesByResourceAttr(batches ptrace.Traces, resourceKeys []string) (map[routingKey][]routingEntry, error) {
	// This function batches all the ResourceSpans with the same routing resource attribute value into a single ptrace.Trace
	// This returns a map which contains list of routing entries containing the routing key, routing key value and the trace
	// There should be a 1:1 mapping between key value <-> trace
	// This is because we group all Resource Spans with the same key value under a single trace
	// We place the routingEntry lists inside a map so we don't have to loop through the list again
	// and regroup by routingKey when we do the batching
	var result = make(map[routingKey][]routingEntry)
	result[traceIDRouting] = []routingEntry{}
	result[resourceAttrRouting] = []routingEntry{}

	rss := batches.ResourceSpans()

	// This is a mapping between the resource attribute values found and the constructed trace
	routeMap := make(map[string]ptrace.Traces)

	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		if keyValue, ok := getResourceAttrValue(rs, resourceKeys); ok {
			// Check if this keyValue has previously been seen
			// if not it constructs an empty ptrace.Trace
			if _, ok := routeMap[keyValue]; !ok {
				routeMap[keyValue] = ptrace.NewTraces()
			}
			rs.CopyTo(routeMap[keyValue].ResourceSpans().AppendEmpty())
		} else {
			// If none of the resource attributes have been found
			// We fallback to routing the given Resource Span by Trace ID
			t := ptrace.NewTraces()
			rs.CopyTo(t.ResourceSpans().AppendEmpty())
			// We can't route this whole Resource Span by a single trace ID
			// because it's possible for the spans under the RS to have different trace IDs
			result[traceIDRouting] = append(result[traceIDRouting], routingEntry{
				routingKey: traceIDRouting,
				trace:      t,
			})
		}
	}

	// We convert the attr value:trace mapping into a list of routingEntries
	// Only traces that will be resource attribute balanced end up in routeMap
	for key, trace := range routeMap {
		result[resourceAttrRouting] = append(result[resourceAttrRouting], routingEntry{
			routingKey: resourceAttrRouting,
			keyValue:   key,
			trace:      trace,
		})
	}

	if len(result[traceIDRouting]) == 0 {
		delete(result, traceIDRouting)
	}

	return result, nil
}

func routeByTraceId(td ptrace.Traces) (string, error) {
	// This function assumes that you are receiving a single trace i.e. single traceId
	// returns the traceId as the routing key
	rs := td.ResourceSpans()
	if rs.Len() == 0 {
		return "", errors.New("empty resource spans")
	}
	if rs.Len() > 1 {
		return "", errors.New("routeByTraceId must receive a ptrace.Traces with a single ResourceSpan")
	}
	ils := rs.At(0).ScopeSpans()
	if ils.Len() == 0 {
		return "", errors.New("empty scope spans")
	}
	spans := ils.At(0).Spans()
	if spans.Len() == 0 {
		return "", errors.New("empty spans")
	}
	tid := spans.At(0).TraceID()
	return string(tid[:]), nil
}
