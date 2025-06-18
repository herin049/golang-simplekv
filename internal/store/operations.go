package store

type SetOperation struct {
	Key   string
	Value string
}

type SetOperationResult struct{}

type GetOperation struct {
	Key string
}

type GetOperationResult struct {
	Value string
}

type DelOperation struct {
	Key string
}

type DelOperationResult struct{}
