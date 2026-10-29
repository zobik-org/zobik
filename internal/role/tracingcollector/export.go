package tracingcollector

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"zobik.org/zobik/internal/bus"
)

// Backend is where the role exports the spans: the tenant's tracing backend
// (implementation §1.2.6, §1.2.17).
type Backend interface {
	// Reach returns once the backend can be reached, or ctx ends.
	Reach(ctx context.Context) error
	ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error
}

// otlpBackend exports over OTLP gRPC with the SDK's exporter. The role keeps
// what was not accepted and exports it again, so the exporter does not retry.
type otlpBackend struct {
	conn *grpc.ClientConn
	*otlptrace.Exporter
}

// Dial is the backend at addr. It goes in plain text: within one host, the
// backend's alias is assigned only by whoever creates the containers
// (implementation §6, The off-Bus interfaces).
func Dial(ctx context.Context, addr string) (Backend, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{Enabled: false}))
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &otlpBackend{conn: conn, Exporter: exp}, nil
}

func (b *otlpBackend) Reach(ctx context.Context) error {
	b.conn.Connect()
	for {
		s := b.conn.GetState()
		if s == connectivity.Ready {
			return nil
		}
		if !b.conn.WaitForStateChange(ctx, s) {
			return ctx.Err()
		}
	}
}

// scope names what produces the spans.
var scope = instrumentation.Scope{Name: "zobik.org/zobik/internal/role/tracingcollector"}

// readOnly is the span as OTel exports it, under the network's resource
// (implementation §1.2.6).
func (s *span) readOnly(traceID string, res *resource.Resource) (sdktrace.ReadOnlySpan, error) {
	tid, err := oteltrace.TraceIDFromHex(traceID)
	if err != nil {
		return nil, err
	}
	sid, err := oteltrace.SpanIDFromHex(s.ID)
	if err != nil {
		return nil, err
	}
	stub := tracetest.SpanStub{
		Name: "root",
		SpanContext: oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: tid, SpanID: sid, TraceFlags: oteltrace.FlagsSampled,
		}),
		SpanKind:             oteltrace.SpanKindInternal,
		StartTime:            s.Start,
		EndTime:              s.End,
		Attributes:           s.attributes(),
		Resource:             res,
		InstrumentationScope: scope,
	}
	if s.Parent != "" && !s.Root {
		pid, err := oteltrace.SpanIDFromHex(s.Parent)
		if err != nil {
			return nil, err
		}
		stub.Parent = oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
			TraceID: tid, SpanID: pid, TraceFlags: oteltrace.FlagsSampled, Remote: true,
		})
	}
	if a := s.Announcement; a != nil && a.Data.Topic != nil && !s.Root {
		stub.Name = *a.Data.Topic
	}
	if s.Outcome != outcomeCompleted {
		stub.Status = sdktrace.Status{Code: codes.Error, Description: s.Outcome}
	}
	for _, f := range s.Events {
		stub.Events = append(stub.Events, sdktrace.Event{Name: f.Type, Time: f.Time, Attributes: eventAttributes(f)})
	}
	return stub.Snapshot(), nil
}

// attributes are the span's (implementation §1.2.6). An absent value is an
// absent attribute.
func (s *span) attributes() []attribute.KeyValue {
	var kv []attribute.KeyValue
	str := func(k string, v *string) {
		if v != nil {
			kv = append(kv, attribute.String(k, *v))
		}
	}
	kv = append(kv, attribute.String("zobik.task_id", s.Task))
	if a := s.Announcement; a != nil {
		if !s.Root {
			str("zobik.topic", a.Data.Topic)
			str("zobik.task_text", a.Data.TaskText)
		}
		str("zobik.workspace", a.Data.Workspace)
		if a.ConfigVersion != "" {
			kv = append(kv, attribute.String("zobik.config_version", a.ConfigVersion))
		}
		if a.Data.CurrentAttempts != nil {
			kv = append(kv, attribute.Int64("zobik.current_attempts", *a.Data.CurrentAttempts))
		}
		str("zobik.previous_attempt", a.Data.PreviousAttempt)
	}
	if g := s.Assigned; g != nil {
		kv = append(kv, attribute.String("zobik.node_id", g.Node))
		if g.Data.Similarity != nil {
			kv = append(kv, attribute.Float64("zobik.similarity", *g.Data.Similarity))
		}
	}
	if s.Outcome != "" {
		kv = append(kv, attribute.String("zobik.outcome", s.Outcome))
	}
	if t := s.Terminal; t != nil {
		str("zobik.failure_code", t.Data.FailureCode)
		str("zobik.failure_reason", t.Data.FailureReason)
		if m := t.Data.Metrics; m != nil {
			kv = append(kv,
				attribute.Int64("zobik.metrics.tokens_in", m.TokensIn),
				attribute.Int64("zobik.metrics.tokens_out", m.TokensOut),
				attribute.Float64("zobik.metrics.cost", m.Cost),
				attribute.Int64("zobik.metrics.billing_errors", m.BillingErrors),
				attribute.Int64("zobik.metrics.duration_ms", m.DurationMs))
		}
	}
	return kv
}

// eventAttributes are those of a span event: who proposed or lost and with
// what similarity, a notice's kind, who sent a late terminal event
// (implementation §1.2.6).
func eventAttributes(f *fact) []attribute.KeyValue {
	var kv []attribute.KeyValue
	switch f.Type {
	case bus.TypeProposed:
		kv = append(kv, attribute.String("zobik.node_id", f.Node))
		if f.Data.DeclaredSimilarity != nil {
			kv = append(kv, attribute.Float64("zobik.declared_similarity", *f.Data.DeclaredSimilarity))
		}
	case bus.TypeRejected:
		kv = append(kv, attribute.String("zobik.node_id", f.Node))
		if f.Data.Similarity != nil {
			kv = append(kv, attribute.Float64("zobik.similarity", *f.Data.Similarity))
		}
	case bus.TypeNotice:
		if f.Data.Kind != nil {
			kv = append(kv, attribute.String("zobik.kind", *f.Data.Kind))
		}
	case bus.TypeCompleted, bus.TypeFailed, bus.TypeAborted:
		kv = append(kv, attribute.String("zobik.node_id", f.Source))
	}
	return kv
}

// networkResource is the resource of every span: service.name is the network's
// name (implementation §1.2.6).
func networkResource(network string) (*resource.Resource, error) {
	if network == "" {
		return nil, fmt.Errorf("tracing_collector: the resource needs the network's name")
	}
	return resource.NewSchemaless(attribute.String("service.name", network)), nil
}
