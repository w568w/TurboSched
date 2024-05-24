package common

import (
	"context"
	"errors"
	"golang.org/x/sync/semaphore"
	"math"
	"sync"
)

// TaskCanceler is a helper struct to manage task cancellation.
type TaskCanceler struct {
	// the context to signal tasks to be canceled.
	taskContext context.Context
	// prevent concurrent cancellation.
	cancelAllLock sync.Mutex
	// the function to cancel all tasks, paired with taskContext.
	cancelAll context.CancelFunc
	// report the number of running routines.
	runningRoutineCount *semaphore.Weighted
}

func NewTaskCanceler(alreadyCanceled bool) *TaskCanceler {
	taskContext, cancelAll := context.WithCancel(context.Background())
	if alreadyCanceled {
		cancelAll()
	}
	return &TaskCanceler{
		taskContext:         taskContext,
		cancelAllLock:       sync.Mutex{},
		cancelAll:           cancelAll,
		runningRoutineCount: semaphore.NewWeighted(math.MaxInt64),
	}
}

// HasCanceled returns true if the task has been canceled.
func (t *TaskCanceler) HasCanceled() bool {
	return errors.Is(t.taskContext.Err(), context.Canceled)
}

// RoutineRegister is used to register a routine to be canceled, which returns a context that triggers when cancellation is requested.
// An optional context can be passed in to merge with the taskContext.
//
// The routine should call RoutineUnregister to claim that it is canceled or done.
func (t *TaskCanceler) RoutineRegister(optionalCxt context.Context) context.Context {
	// to tell the truth, runningRoutineCount can never run out - How can you run out of math.MaxInt64?
	err := t.runningRoutineCount.Acquire(context.Background(), 1)
	if err != nil {
		panic(err) // should never happen
	}
	if optionalCxt != nil {
		mergedCxt, _ := WithBothCxt(t.taskContext, optionalCxt)
		return mergedCxt
	} else {
		return t.taskContext
	}
}

// RoutineUnregister is used to claim that a routine is done.
func (t *TaskCanceler) RoutineUnregister() {
	t.runningRoutineCount.Release(1)
}

// CancelAndWaitAllRoutine cancels all routines and waits for them to finish.
// Returns true if all routines are successfully canceled. False if the task is already canceled.
func (t *TaskCanceler) CancelAndWaitAllRoutine() (ok bool) {
	t.cancelAllLock.Lock()
	defer t.cancelAllLock.Unlock()
	if t.HasCanceled() {
		return false
	}
	t.cancelAll()
	err := t.runningRoutineCount.Acquire(context.Background(), math.MaxInt64)
	if err != nil {
		panic(err) // should never happen
	}
	return true
}
