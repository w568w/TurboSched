package common

import (
	"errors"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"runtime/debug"
	"strings"
	"time"
	pb "turbo_sched/common/proto"
)

// UError is a unified error struct for all errors passed between components.
//
// You should always return a UError or a context.Canceled error from a gRPC service function, any other error should be wrapped in a UError.
// You should never create a UError directly. Instead, use NewError, WrapError or FromGRPCError.
type UError struct {
	// Message is a concise and human-readable error message.
	// It can provide extra information about the error besides the error code.
	// Due to the length limit of gRPC Status.Details, it should be short.
	// It does NOT need to be unique or stable.
	Message string
	// Code is a unique error code for the error.
	// It should be stable and unique across all components.
	Code UCode

	// Time is the UTC time when the error occurred.
	OccurredTime time.Time

	// StackTrace is the stack trace of the error.
	// It is optional and can be nil.
	StackTrace *[]byte

	// OriginalError is the original error.
	// It is optional and can be nil.
	OriginalError error
}

//go:generate stringer -type=UCode
type UCode uint32

const (
	// UCodeDatabase is for database errors.
	UCodeDatabase UCode = 1
	// UCodeDialNode is for dialing node errors.
	UCodeDialNode UCode = 2
	// UCodeAssignTask is for assigning task errors.
	UCodeAssignTask UCode = 3
	// UCodeInvalidPeerInfo means unable to extract valid peer info from a request, e.g. missing peer info or invalid format.
	UCodeInvalidPeerInfo UCode = 4
	// UCodeTaskNotFound means the requested task is not found.
	UCodeTaskNotFound UCode = 5
	// UCodeMalformedVariant means the request is malformed, specifically, the variant field in the request is not recognized.
	UCodeMalformedVariant UCode = 6
	// UCodeStreamError means an error occurred when streaming data.
	UCodeStreamError UCode = 7
	// UCodeWrongTaskState means the task is in a wrong state for the operation, e.g. trying to cancel a task that is already completed.
	UCodeWrongTaskState UCode = 8
	// UCodeSystemInternal is for internal system errors, e.g. unable to open a TTY.
	UCodeSystemInternal UCode = 9
)

// NewError creates a new UError with the given code and message.
func NewError(code UCode, message string, withStacktrace bool) *UError {
	return WrapError(code, message, nil, withStacktrace)
}

// WrapError wraps the given error with a UError.
func WrapError(code UCode, message string, err error, withStacktrace bool) *UError {
	var runStack *[]byte
	if withStacktrace {
		stack := debug.Stack()
		runStack = &stack
	}
	return &UError{
		Message:       message,
		Code:          code,
		StackTrace:    runStack,
		OriginalError: err,
		OccurredTime:  time.Now().UTC(),
	}
}

// Error implements the error interface.
func (e *UError) Error() string {
	return e.Message
}

// String returns a string representation of the UError. It is only for debugging, and not machine-readable.
func (e *UError) String() string {
	strBuilder := strings.Builder{}
	strBuilder.WriteString(fmt.Sprintf("UError{Code: %s, Message: %s, OccurredTime: %s", e.Code, e.Message, e.OccurredTime))
	if e.OriginalError != nil {
		strBuilder.WriteString(fmt.Sprintf(", OriginalError: %s", e.OriginalError))
	}
	if e.StackTrace != nil {
		strBuilder.WriteString(", StackTrace: ")
		strBuilder.Write(*e.StackTrace)
	}
	strBuilder.WriteString("}")
	return strBuilder.String()
}

// GRPCStatus converts the UError to a gRPC status.
// If error occurred when creating the gRPC status, it will return a wrapped error (machine-unreadable).
func (e *UError) GRPCStatus() *status.Status {
	// Define a suitable code for the error.
	// Normally, we use codes.Unknown for all errors.
	// But for some special errors (e.g., context.Canceled), we should use the corresponding code.
	var suitableGRPCCode codes.Code = codes.Unknown

	var orginalError *string = nil
	if e.OriginalError != nil {
		errStr := e.OriginalError.Error()
		orginalError = &errStr
		// detect context's error and use the corresponding gRPC code
		suitableGRPCCode = status.FromContextError(e.OriginalError).Code()
	}

	st, err := status.New(suitableGRPCCode, e.Message).WithDetails(
		&pb.UErrorEx{
			OriginalError: orginalError,
			OccurredTime: &timestamppb.Timestamp{
				Seconds: e.OccurredTime.Unix(),
				Nanos:   int32(e.OccurredTime.Nanosecond()),
			},
			Code: uint32(e.Code),
		})
	if err != nil {
		return status.New(codes.Unknown, fmt.Sprintf("Unable to create gRPC status: %s (original message: %s, code: %s, error: %v)", err.Error(), e.Message, e.Code, orginalError))
	} else {
		return st
	}
}

// FromGRPCError converts a gRPC error to a UError.
func FromGRPCError(err error) (UError, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return UError{}, false
	}
	if len(st.Details()) == 0 {
		return UError{}, false
	}
	details := st.Details()[0]
	uerr, ok := details.(*pb.UErrorEx)
	if !ok {
		return UError{}, false
	}

	var originalError error = nil
	if uerr.OriginalError != nil {
		originalError = errors.New(*uerr.OriginalError)
	}
	return UError{
		Message: st.Message(),
		Code:    UCode(uerr.Code),
		OccurredTime: time.Unix(
			uerr.OccurredTime.Seconds,
			int64(uerr.OccurredTime.Nanos)).UTC(),
		StackTrace:    nil,
		OriginalError: originalError,
	}, true
}

// IsGrpcCanceled returns true if the given error is about a canceled Grpc call.
func IsGrpcCanceled(err error) bool {
	return status.Code(err) == codes.Canceled
}
