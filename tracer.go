package platform

import (
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Tracer interface {
	Start(parentTrace *Trace, name string) *Trace
	End(trace *Trace)
}

type PublishingTracer struct {
	publisher Publisher
	traces    chan *Trace
}

func (t *PublishingTracer) runEmitter() {
	traceList := &TraceList{}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case trace := <-t.traces:
			traceList.Traces = append(traceList.Traces, trace)

		case <-ticker.C:
			if len(traceList.Traces) > 50 {
				if traceListBytes, err := Marshal(traceList); err == nil {
					t.publisher.Publish("platform.trace_list", traceListBytes)
				}

				// Reset the trace list
				traceList = &TraceList{}
			}
		}
	}
}

func (t *PublishingTracer) Start(parentTrace *Trace, name string) *Trace {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	newTrace := &Trace{
		Uuid:      String(strings.Replace(CreateUUID(), "-", "", -1)),
		Name:      String(name),
		SpanUuid:  String(strconv.Itoa(r.Int())),
		StartTime: String(time.Now().Format(time.RFC3339Nano)),
	}

	if parentTrace.GetUuid() != "" {
		newTrace.Uuid = String(parentTrace.GetUuid())
		newTrace.ParentSpanUuid = String(parentTrace.GetSpanUuid())
	}

	t.traces <- newTrace

	return newTrace
}

func (t *PublishingTracer) End(trace *Trace) {
	trace.EndTime = String(time.Now().Format(time.RFC3339Nano))

	t.traces <- trace
}

func NewPublishingTracer(publisher Publisher) *PublishingTracer {
	publishingTracer := &PublishingTracer{
		publisher: publisher,
		traces:    make(chan *Trace, 50),
	}

	go publishingTracer.runEmitter()

	return publishingTracer
}
