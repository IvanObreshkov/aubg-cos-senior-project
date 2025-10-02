package internal

import (
	"context"
	"fmt"
)

// https://adithayyil.tech/posts/go-type-safe-contexts/

// CtxKey represents a type-safe context key
type CtxKey[T any] struct {
	name string
}

// NewCtxKey creates a new typed context key
func NewCtxKey[T any](name string) CtxKey[T] {
	return CtxKey[T]{name: name}
}

// String implements fmt.Stringer for debugging
func (k CtxKey[T]) String() string {
	return fmt.Sprintf("Key[%T](%s)", *new(T), k.name)
}

// SetCtxKey stores a value in the context with type safety
func SetCtxKey[T any](ctx context.Context, key CtxKey[T], value T) context.Context {
	return context.WithValue(ctx, key, value)
}

// GetCtxKey retrieves a value from the context with type safety
func GetCtxKey[T any](ctx context.Context, key CtxKey[T]) (T, bool) {
	value, ok := ctx.Value(key).(T)
	return value, ok
}
