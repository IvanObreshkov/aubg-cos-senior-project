package raft

import (
	"math/rand"
)

// ElectionTimeoutMs is the allowed period of time in Ms for a follower not to receive communications from a Leader,
// as defined in Section 5.2 from the [Raft paper](https://raft.github.io/raft.pdf). If no communications are received
// by the Follower over this time period, it assumes no viable Leader exists or is available and initiates an election
// to choose a new one.
func GetElectionTimeoutMs() int {
	// 300-150 gives the size of the range, and we add 1 to make it inclusive
	// We add 150 to shift the range, as rand.Intn() could return 0
	return rand.Intn(300-150+1) + 150
}
