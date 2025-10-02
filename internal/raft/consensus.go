package raft

/*
These are notes from Section 5.2
Begin an Election:
A follower server increments currTerm and changes state to Candidate

The follower votes for itself and issues a ReqVote RPCs in parallel to each of the servers in the cluster

Possible outcomes:
1. A candite wins the election if it receives votes from the majority of the servers in the cluster for the same term.
Majority is defined as `floor(n/2) + 1`. When a server wins the election it becomes a leader, and sends heartbeat
messages to all servers in the cluster, to notify them and prevent new elections
Section 5.4 for more details

2. While a server waits for votes it may receive an AppendEntries RPC (heartbeat) from a server claiming to be a
leader If the `term` included in the RPC is >= then the candidate server (the current one), then the server moves from
candidate to follower again. Else the candidate rejects the RPC and maintains the candidate state.4

3. The candidate neither wins nor looses the election. This could happen if many servers go from F -> C at the same
time and votes are split, so that no C obtains a majority. In this case an election timeout occurs and each C will
initiate a new election by incrementing the currTem and sending another round of ReqVote RPCs. A random election
timeout is chosen for each server to prevent indefinite split votes. The randomised election timeout of each server is
restarted at the beginning of a new election

Section 6
To prevent this problem, servers disregard RequestVote
RPCs when they believe a current leader exists. Specifically, if a server receives a RequestVote RPC within
the minimum election timeout of hearing from a current leader, it does not update its term or grant its vote

*/

// ConsensusModule on a server receives commands from clients and adds them to its log
type ConsensusModule struct {
}
