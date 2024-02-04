package common

// Some misc functions that are used in multiple places, but don't really belong
// anywhere else.

// Void is a type that can be used to indicate that a function returns nothing.
type Void struct{}

var VOID Void

// Map applies the provided function to each element of the slice and returns a new slice.
func Map[T, U any](input []T, fn func(T) U) []U {
	result := make([]U, len(input))
	for i, v := range input {
		result[i] = fn(v)
	}
	return result
}

// Filter returns a new slice containing only the elements that satisfy the given condition.
func Filter[T any](input []T, condition func(T) bool) []T {
	var result []T
	for _, v := range input {
		if condition(v) {
			result = append(result, v)
		}
	}
	return result
}
