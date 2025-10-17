package ctxpropagation

import (
	"context"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

type (
	// propagator implements the custom context propagator
	propagator struct{}
)

// NewContextPropagator returns a context propagator that propagates a set of
// string key-value pairs across a workflow
func NewContextPropagator() workflow.ContextPropagator {
	return &propagator{}
}

// Inject injects values from context into headers for propagation
func (s *propagator) Inject(ctx context.Context, writer workflow.HeaderWriter) error {
	value := ctx.Value("nexus-endpoint")
	payload, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		return err
	}
	writer.Set("nexus-endpoint", payload)
	return nil
}

// InjectFromWorkflow injects values from context into headers for propagation
func (s *propagator) InjectFromWorkflow(ctx workflow.Context, writer workflow.HeaderWriter) error {
	value := ctx.Value("nexus-endpoint")
	payload, err := converter.GetDefaultDataConverter().ToPayload(value)
	if err != nil {
		return err
	}
	writer.Set("nexus-endpoint", payload)
	return nil
}

// Extract extracts values from headers and puts them into context
func (s *propagator) Extract(ctx context.Context, reader workflow.HeaderReader) (context.Context, error) {
	if value, ok := reader.Get("nexus-endpoint"); ok {
		var endpoint string
		if err := converter.GetDefaultDataConverter().FromPayload(value, &endpoint); err != nil {
			return ctx, nil
		}
		ctx = context.WithValue(ctx, "nexus-endpoint", endpoint)
	}

	return ctx, nil
}

// ExtractToWorkflow extracts values from headers and puts them into context
func (s *propagator) ExtractToWorkflow(ctx workflow.Context, reader workflow.HeaderReader) (workflow.Context, error) {
	if value, ok := reader.Get("nexus-endpoint"); ok {
		var endpoint string
		if err := converter.GetDefaultDataConverter().FromPayload(value, &endpoint); err != nil {
			return ctx, nil
		}
		ctx = workflow.WithValue(ctx, "nexus-endpoint", endpoint)
	}

	return ctx, nil
}
