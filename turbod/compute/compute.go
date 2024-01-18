package compute

import (
	"context"
	"fmt"
	"turbo_sched/common"
)

type ComputeInterface struct {
}

func (c *ComputeInterface) TaskAssign(ctx context.Context, submission *common.TaskAssignInfo, reply *bool) error {
	fmt.Println("TaskAssign", submission)

	*reply = true
	return nil
}
