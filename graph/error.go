package graph

type GraphCodeError string

const (
	ErrDuplicateNode          GraphCodeError = "node with same key already exists in this graph"
	ErrConnectNotExistingNode GraphCodeError = "node to connect does not exist in this graph"
)

func (ge GraphCodeError) Error() string {
	return string(ge)
}

type GraphError struct {
	Code    GraphCodeError
	Message string
}

func NewGraphError(code GraphCodeError, message string) *GraphError {
	return &GraphError{
		Code:    code,
		Message: message,
	}
}

func (ge *GraphError) Error() string {
	return ge.Code.Error() + ": " + ge.Message
}

func (ge *GraphError) Unwrap() error {
	return ge.Code
}
