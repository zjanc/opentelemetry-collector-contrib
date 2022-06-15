package filterspanprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestSpanFilter(t *testing.T) {
	// Setup
	ptrace.NewTraces()
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.ScopeSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()

	currentSpan := ss.Spans().AppendEmpty()
	futureSpan := ss.Spans().AppendEmpty()
	pastSpan := ss.Spans().AppendEmpty()

	currentTime := time.Now()
	currentSpan.SetName("test-service")
	currentSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(currentTime))

	futureTime := time.Now().Add(time.Hour * 2)
	futureSpan.SetName("test-future-service")
	futureSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(futureTime))

	pastTime := time.Now().Add(time.Hour * -2)
	pastSpan.SetName("test-past-service")
	pastSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(pastTime))

	// Test
	traceSink := new(consumertest.TracesSink)
	cfg := &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentIDWithName(typeStr, typeStr)),
		// 1 hour offset
		StartTimeOffset: 3600000000000,
	}
	processor, _ := newFilterSpanProcessor(traceSink, cfg)
	err := processor.ConsumeTraces(context.Background(), traces)

	// Validate
	assert.NoError(t, err)
	assert.Len(t, traceSink.AllTraces(), 1)

	// Assert that the correct spans got dropped
	unfilteredSpans := traceSink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, "test-service", unfilteredSpans.Name())
}
