package store

type StoreRequest struct {
	Operation any
}

type StoreResponse struct {
	Result   any
	ErrorMsg string
}
