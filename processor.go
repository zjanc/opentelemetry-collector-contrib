package filterspanprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

type filterspan struct {
	StartTimeOffset uint64
	nextConsumer    consumer.Traces
}

func newFilterSpanProcessor(nextConsumer consumer.Traces, cfg *Config) (component.TracesProcessor, error) {
	fsp := filterspan{
		StartTimeOffset: cfg.StartTimeOffset,
		nextConsumer:    nextConsumer,
	}
	return processorhelper.NewTracesProcessor(
		cfg,
		nextConsumer,
		fsp.ProcessTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
	)
}

func (fsp *filterspan) ProcessTraces(_ context.Context, batch ptrace.Traces) (ptrace.Traces, error) {
	timeOffset := time.Nanosecond * time.Duration(fsp.StartTimeOffset)
	lowerBound := time.Now().Add(timeOffset * -1)
	upperBound := time.Now().Add(timeOffset)
	batch.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(s ptrace.Span) bool {
				// This only accepts spans that are in the time range
				// [time.Now() - offset, time.Now() + offset]
				// Expression below checks if timestamp is out of bounds and drops if yes
				oob := s.StartTimestamp().AsTime().Before(lowerBound) || s.StartTimestamp().AsTime().After(upperBound)
				return oob
			})
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})
	if batch.ResourceSpans().Len() == 0 {
		return batch, processorhelper.ErrSkipProcessingData
	}

	return batch, nil
}
