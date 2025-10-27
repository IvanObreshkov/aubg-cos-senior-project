package state_machine

import "aubg-cos-senior-project/internal/raft/proto"

// StateMachine is an interface representing the StateMachine of the Server defined in Section 2 from the
// [Raft paper](https://raft.github.io/raft.pdf). It is inspired from the FSM interface defined in
// [Hashicorp's Raft impl](https://github.com/hashicorp/raft/blob/main/fsm.go)
type StateMachine interface {
	Apply(log []proto.LogEntry)
	// TODO: Add Snapshot and Restore methods
}
