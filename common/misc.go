package common

import (
	"context"
)

// Some misc functions that are used in multiple places, but don't really belong
// anywhere else.

// Map applies the provided function to each element of the slice and returns a new slice.
func Map[S ~[]T, T, U any](input S, fn func(T) U) []U {
	result := make([]U, len(input))
	for i, v := range input {
		result[i] = fn(v)
	}
	return result
}

// Filter returns a new slice containing only the elements that satisfy the given condition.
func Filter[S ~[]T, T any](input S, condition func(T) bool) []T {
	var result []T
	for _, v := range input {
		if condition(v) {
			result = append(result, v)
		}
	}
	return result
}

// RemoveUnordered removes the element at the given index from the slice without preserving the order.
// It is very fast i.e. O(1), but the order of the elements in the slice will change.
func RemoveUnordered[S ~[]E, E any](input S, index int) (S, E) {
	removed := input[index]
	input[index] = input[len(input)-1]
	return input[:len(input)-1], removed
}

// IsPowerOfTwo returns true if the given positive number is a power of two.
func IsPowerOfTwo(n int) bool {
	return n > 0 && n&(n-1) == 0
}

// WithBothCxt merges two contexts into one. If one context is canceled, the returned context will be canceled as well.
// Additionally, both contexts are canceled when mergeCancel is called.
//
// Special behavior:
// If ctx1 has already been canceled, mergedContext will be canceled immediately;
// if ctx2 has already been canceled, mergedContext will be canceled in a goroutine immediately.
func WithBothCxt(ctx1, ctx2 context.Context) (mergedContext context.Context, mergeCancel context.CancelFunc) {
	wrappedCtx1, cancelWrappedCtx1 := context.WithCancelCause(ctx1)
	cancelWrappedCtx2 := context.AfterFunc(ctx2, func() {
		cancelWrappedCtx1(context.Cause(ctx2))
	})
	return wrappedCtx1, func() {
		cancelWrappedCtx2()
		// it is necessary to call cancelWrappedCtx1 again,
		// because cancelWrappedCtx2 can fail if ctx2 is already canceled.
		// In that case, cancelWrappedCtx1 in AfterFunc will not be called.
		cancelWrappedCtx1(context.Canceled)
	}
}
