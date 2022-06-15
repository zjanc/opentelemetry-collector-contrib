package filterspanprocessor

import (
	"context"

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
	return batch, nil
}
