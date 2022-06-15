package filterspanprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var _ component.TracesProcessor = (*filterSpan)(nil)

type filterSpan struct {
	next consumer.Traces
}

func (f *filterSpan) ConsumeTraces(ctx context.Context, batch ptrace.Traces) error {
	return nil
}

func (f *filterSpan) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (f *filterSpan) Start(_ context.Context, host component.Host) error {
	return nil
}

func (f *filterSpan) Shutdown(context.Context) error {
	return nil
}
