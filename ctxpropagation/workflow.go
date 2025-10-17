package ctxpropagation

import (
	"go.temporal.io/sdk/workflow"
)

// CtxPropWorkflow workflow definition
func CtxPropWorkflow(ctx workflow.Context, input string) (string, error) {
	return "result", nil
}
