package common

// Some misc functions that are used in multiple places, but don't really belong
// anywhere else.

// Void is a type that can be used to indicate that a function returns nothing.
type Void struct{}

var VOID Void

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
