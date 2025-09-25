package server

import (
	"aubg-cos-senior-project/internal"
	"context"
)

var (
	serverCurrTerm = internal.NewCtxKey[uint64]("currTerm")
	serverID       = internal.NewCtxKey[ServerID]("serverID")
	serverAddr     = internal.NewCtxKey[ServerAddress]("serverAddr")
)

func SetServerCurrTerm(ctx context.Context, currTerm uint64) context.Context {
	return internal.SetCtxKey(ctx, serverCurrTerm, currTerm)
}

func GetServerCurrTerm(ctx context.Context) (uint64, bool) {
	return internal.GetCtxKey(ctx, serverCurrTerm)
}
func SetServerID(ctx context.Context, id ServerID) context.Context {
	return internal.SetCtxKey(ctx, serverID, id)
}

func GetServerID(ctx context.Context) (ServerID, bool) {
	return internal.GetCtxKey(ctx, serverID)
}

func SetServerAddr(ctx context.Context, addr ServerAddress) context.Context {
	return internal.SetCtxKey(ctx, serverAddr, addr)
}

func GetServerAddr(ctx context.Context) (ServerAddress, bool) {
	return internal.GetCtxKey(ctx, serverAddr)
}
