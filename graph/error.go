package graph

type GraphErrorCode string

const (
	ErrDuplicateNode          GraphErrorCode = "node with same key already exists in this graph"
	ErrConnectNotExistingNode GraphErrorCode = "node to connect does not exist in this graph"
)

func (ge GraphErrorCode) Error() string {
	return string(ge)
}

type GraphError struct {
	Code    GraphErrorCode
	Message string
}

func (ge *GraphError) Error() string {
	return ge.Code.Error() + ": " + ge.Message
}

func (ge *GraphError) Unwrap() error {
	return ge.Code
}
