package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServerContext_CurrTerm(t *testing.T) {
	ctx := context.Background()

	t.Run("sets and gets current term", func(t *testing.T) {
		ctx = SetServerCurrTerm(ctx, 42)

		term, ok := GetServerCurrTerm(ctx)
		assert.True(t, ok)
		assert.Equal(t, uint64(42), term)
	})

	t.Run("returns false for missing term", func(t *testing.T) {
		ctx := context.Background()

		_, ok := GetServerCurrTerm(ctx)
		assert.False(t, ok)
	})
}

func TestServerContext_ServerID(t *testing.T) {
	ctx := context.Background()

	t.Run("sets and gets server ID", func(t *testing.T) {
		ctx = SetServerID(ctx, "server-123")

		id, ok := GetServerID(ctx)
		assert.True(t, ok)
		assert.Equal(t, ServerID("server-123"), id)
	})

	t.Run("returns false for missing ID", func(t *testing.T) {
		ctx := context.Background()

		_, ok := GetServerID(ctx)
		assert.False(t, ok)
	})
}

func TestServerContext_ServerAddr(t *testing.T) {
	ctx := context.Background()

	t.Run("sets and gets server address", func(t *testing.T) {
		ctx = SetServerAddr(ctx, "localhost:5001")

		addr, ok := GetServerAddr(ctx)
		assert.True(t, ok)
		assert.Equal(t, ServerAddress("localhost:5001"), addr)
	})

	t.Run("returns false for missing address", func(t *testing.T) {
		ctx := context.Background()

		_, ok := GetServerAddr(ctx)
		assert.False(t, ok)
	})
}

func TestServerContext_AllValues(t *testing.T) {
	t.Run("sets and retrieves all context values", func(t *testing.T) {
		ctx := context.Background()

		ctx = SetServerCurrTerm(ctx, 10)
		ctx = SetServerID(ctx, "server-xyz")
		ctx = SetServerAddr(ctx, "192.168.1.1:5001")

		term, termOk := GetServerCurrTerm(ctx)
		id, idOk := GetServerID(ctx)
		addr, addrOk := GetServerAddr(ctx)

		assert.True(t, termOk)
		assert.Equal(t, uint64(10), term)

		assert.True(t, idOk)
		assert.Equal(t, ServerID("server-xyz"), id)

		assert.True(t, addrOk)
		assert.Equal(t, ServerAddress("192.168.1.1:5001"), addr)
	})
}
