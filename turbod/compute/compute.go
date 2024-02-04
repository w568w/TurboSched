package compute

import (
	"context"
	"fmt"
	"github.com/smallnest/rpcx/client"
	"os/exec"
	"turbo_sched/common"
)

type ComputeInterface struct {
	ControlClient client.XClient
}

func (c *ComputeInterface) TaskAssign(ctx context.Context, submission *common.TaskAssignInfo, reply *common.Void) error {
	fmt.Println("TaskAssign", submission)

	go func() {
		cmd := exec.Command(submission.CommandLine.Program, submission.CommandLine.Args...)
		cmd.Env = append(cmd.Env, submission.CommandLine.Env...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Println("TaskAssign failed:", err)
			return
		}
		err = c.ControlClient.Call(context.Background(), "ReportTask", common.TaskReportInfo{
			ID:       submission.ID,
			Output:   output,
			ExitCode: cmd.ProcessState.ExitCode(),
		}, &common.VOID)
		if err != nil {
			panic(err)
		}
	}()
	*reply = common.VOID
	return nil
}
