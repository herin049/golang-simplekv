package store

type stringError string

func (e stringError) Error() string {
	return string(e)
}
