package filterspanprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
)

var _ component.TracesProcessor = (*filterSpan)(nil)

type filterSpan struct {
	next consumer.Traces
}

func (f *filterSpan) ConsumeTraces(ctx context.Context, batch pdata.Traces) error {
	return nil
}

func (f *filterSpan) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: true}
}

func (f *filterSpan) Start(_ context.Context, host component.Host) error {
	return nil
}

func (f *filterSpan) Shutdown(context.Context) error {
	return nil
}
