package nexuscontextpropagation

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/workflow"
)

type WorkerInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (w *WorkerInterceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	in := &workflowInboundInterceptor{parent: w}
	in.Next = next
	return in
}

func (w *WorkerInterceptor) InterceptNexusOperation(ctx context.Context, next interceptor.NexusOperationInboundInterceptor) interceptor.NexusOperationInboundInterceptor {
	i := &nexusOperationInboundInterceptor{parent: w}
	i.Next = next
	return i
}

type workflowInboundInterceptor struct {
	interceptor.WorkflowInboundInterceptorBase
	parent *WorkerInterceptor
}

func (in *workflowInboundInterceptor) Init(next interceptor.WorkflowOutboundInterceptor) error {
	out := &workflowOutboundInterceptor{parent: in.parent}
	out.Next = next
	return in.Next.Init(out)
}

func (w *workflowInboundInterceptor) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (interface{}, error) {
	endpoint, ok := ctx.Value("nexus-endpoint").(string)
	res, err := w.Next.ExecuteWorkflow(ctx, in)
	if err != nil {
		// TODO: enforce encryption of errors.
		return res, err
	}
	if ok && endpoint != "" {
		return EndpointValue{endpoint, res}, nil
	}
	return res, err
}

type workflowOutboundInterceptor struct {
	interceptor.WorkflowOutboundInterceptorBase
	parent *WorkerInterceptor
}

type nexusErrorFuture struct {
	workflow.Future
}

func newNexusErrorFuture(ctx workflow.Context, err error) nexusErrorFuture {
	fut, settable := workflow.NewFuture(ctx)
	settable.SetError(err)
	return nexusErrorFuture{fut}
}

func (n nexusErrorFuture) GetNexusOperationExecution() workflow.Future {
	// Return the same future
	return n
}

// ExecuteNexusOperation implements interceptor.WorkflowOutboundInterceptor. It extracts values from workflow context
// and propagates them via a Nexus header.
func (out *workflowOutboundInterceptor) ExecuteNexusOperation(
	ctx workflow.Context,
	input interceptor.ExecuteNexusOperationInput,
) workflow.NexusOperationFuture {
	input.Input = EndpointValue{input.Client.Endpoint(), input.Input}
	return out.Next.ExecuteNexusOperation(ctx, input)
}

// nexusOperationInboundInterceptor implements NexusOperationInboundInterceptor to intercept StartOperation.
// Implementation may also implement Init to inject a NexusOperationOutboundInterceptor that can customize logging,
// metrics, and the client, as well as CancelOperation to intercept operation cancelation.
type nexusOperationInboundInterceptor struct {
	interceptor.NexusOperationInboundInterceptorBase
	parent *WorkerInterceptor
}

type EndpointValue struct {
	Endpoint string
	Value    any
}

// StartOperation implements internal.NexusOperationInboundInterceptor. It extracts context propagated via a Nexus
// header into a Go context value.
func (n *nexusOperationInboundInterceptor) StartOperation(ctx context.Context, input interceptor.NexusStartOperationInput) (nexus.HandlerStartOperationResult[any], error) {
	info := temporalnexus.GetOperationInfo(ctx)
	endpoint := "demo-not-the-endpoint-" + info.TaskQueue
	// Propagate via context propagators to workflow.
	ctx = context.WithValue(ctx, "nexus-endpoint", endpoint)
	res, err := n.Next.StartOperation(ctx, input)
	if err != nil {
		// TODO: enforce encryption of errors.
		return nil, err
	}
	switch r := res.(type) {
	case interface{ ValueAsAny() any }:
		return &nexus.HandlerStartOperationResultSync[any]{
			Value: EndpointValue{endpoint, r.ValueAsAny()},
		}, nil
	default:
		return r, nil
	}
}

type dataConverter struct {
	wrapped converter.DataConverter
}

// FromPayloads implements converter.DataConverter.
func (dc dataConverter) FromPayloads(payloads *common.Payloads, valuePtrs ...interface{}) error {
	for i, payload := range payloads.GetPayloads() {
		if err := dc.FromPayload(payload, valuePtrs[i]); err != nil {
			return err
		}
	}
	return nil
}

// ToPayloads implements converter.DataConverter.
func (dc dataConverter) ToPayloads(value ...interface{}) (*common.Payloads, error) {
	payloads := make([]*common.Payload, len(value))
	for i, val := range value {
		payload, err := dc.ToPayload(val)
		if err != nil {
			return nil, err
		}
		payloads[i] = payload
	}
	return &common.Payloads{Payloads: payloads}, nil
}

// ToString implements converter.DataConverter.
func (dc dataConverter) ToString(input *common.Payload) string {
	panic("unimplemented")
}

// ToStrings implements converter.DataConverter.
func (dc dataConverter) ToStrings(input *common.Payloads) []string {
	panic("unimplemented")
}

func (dc dataConverter) ToPayload(value interface{}) (*common.Payload, error) {
	if v, ok := value.(EndpointValue); ok {
		payload, err := dc.wrapped.ToPayload(v.Value)
		if err != nil {
			return nil, err
		}
		if payload.Metadata == nil {
			payload.Metadata = make(map[string][]byte, 1)
		}
		// Add this metadata so the encryption codec can use it to select a specific encryption key.
		payload.Metadata["nexus-endpoint"] = []byte(v.Endpoint)
		fmt.Println(">>>>>>>>>>> toPayload", v.Endpoint, v.Value)
		return payload, nil
	}
	return dc.wrapped.ToPayload(value)
}

func (dc dataConverter) FromPayload(payload *common.Payload, valueptr interface{}) error {
	m := payload.GetMetadata()
	err := dc.wrapped.FromPayload(payload, valueptr)
	if err != nil || m == nil {
		return err
	}
	if endpoint, ok := m["nexus-endpoint"]; ok {
		fmt.Println("<<<<<<<<<<< fromPayload", string(endpoint), reflect.ValueOf(valueptr).Elem().Interface())
	}
	return err
}

func DataConverter() converter.DataConverter {
	return converter.NewCodecDataConverter(
		dataConverter{
			converter.GetDefaultDataConverter(),
		},
		codec{},
	)
}

type codec struct {
}

// Decode implements converter.PayloadCodec.
func (c codec) Decode(payloads []*common.Payload) ([]*common.Payload, error) {
	for _, payload := range payloads {
		if m := payload.GetMetadata(); m != nil {
			if enc := payload.Metadata["encoding"]; slices.Equal(enc[:len([]byte("binary/encoded+"))], []byte("binary/encoded+")) {
				payload.Metadata["encoding"] = enc[len([]byte("binary/encoded+")):]
			}
		}
	}
	return payloads, nil
}

// Encode implements converter.PayloadCodec.
func (c codec) Encode(payloads []*common.Payload) ([]*common.Payload, error) {
	for _, payload := range payloads {
		if m := payload.GetMetadata(); m != nil {
			// if _, ok := m["nexus-endpoint"]; ok {
			// }
			payload.Metadata["encoding"] = append([]byte("binary/encoded+"), m["encoding"]...)
		}
	}
	return payloads, nil
}
