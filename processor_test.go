package filterspanprocessor

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestOne(t *testing.T) {
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

	futureTime := time.Now().Add(time.Hour)
	futureSpan.SetName("test-future-service")
	futureSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(futureTime))

	pastTime := time.Now().Add(time.Hour * -1)
	pastSpan.SetName("test-past-service")
	pastSpan.SetStartTimestamp(pcommon.NewTimestampFromTime(pastTime))

}
