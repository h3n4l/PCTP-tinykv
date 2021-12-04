// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:   c.ID,
		Term: 0, 							// In raft paper, the term of a new raft peer is 0.
		Vote: 0, 							// the config guarantee the id will greater than 0,
											// so the Vote set to 0 can represent that there is
											// no vote in the current state of this node.
		RaftLog:          nil,            	// TODO: how to set RaftLog
		Prs:              nil,            	// TODO: set later, (2AB)
		State:            StateFollower, 	// In raft paper, the init state is candidate.
		votes:            nil,				// set later
		msgs:             nil,				// TODO: how to set
		Lead:             0,				// In raft paper, a new raft peer will have no leader
		heartbeatTimeout: c.HeartbeatTick,	// TODO: check
		electionTimeout:  c.ElectionTick,	// TODO: check
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,				// TODO: 3A leader transfer
		PendingConfIndex: 0,				// TODO: 3A conf change
	}
	r.votes = make(map[uint64]bool)
	r.Prs = make(map[uint64]*Progress)
	for _,id := range c.peers{
		r.votes[id] = false
		r.Prs[id] = nil						// TODO: (2AB)
	}
	// Set random seed
	rand.Seed(time.Now().Unix())
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if to != r.id{
		// send heart beat
		heartbeatMessage := pb.Message{
			MsgType:              pb.MessageType_MsgHeartbeat,
			To:                   to,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              0,			// TODO: modify here in 2AB log replication
			Index:                0,			// TODO: modify here in 2AB log replication
			Entries:              nil,			// TODO: modify here in 2AB log replication
			Commit:               0,			// TODO: modify here in 2AB log replication
			Snapshot:             nil,			// TODO: modify here in 2AC snapshot
			Reject:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}
		r.msgs = append(r.msgs,heartbeatMessage)
	}
}

// sendRequestVote sends a requestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64){
	if to != r.id{
		// send heart beat
		heartbeatMessage := pb.Message{
			MsgType:              pb.MessageType_MsgRequestVote,
			To:                   to,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              0,			// TODO: modify here in 2AB log replication
			Index:                0,			// TODO: modify here in 2AB log replication
			Entries:              nil,			// TODO: modify here in 2AB log replication
			Commit:               0,			// TODO: modify here in 2AB log replication
			Snapshot:             nil,			// TODO: modify here in 2AC snapshot
			Reject:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}
		r.msgs = append(r.msgs,heartbeatMessage)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State{
	case StateFollower:
		r.electionElapsed += 1
		// check the electionElapsed
		if r.electionElapsed >= r.electionTimeout{
			// If this peer had not received the heartbeat during the r.electionTimeout,
			// need become candidate and prepare to start a election
			// Call Step function to handle a message type of MessageType_MsgHup MessageType.
			localHupMsg := r.newMsgHup()
			r.Step(localHupMsg)
		}
	case StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout{
			// ElectionTimeout, start a new election
			// Transfer to a StateCandidate to request votes in the cluster.
			// r.becomeCandidate()
			// Call Step function to handle a message type of MessageType_MsgHup MessageType.
			localHupMsg := r.newMsgHup()
			r.Step(localHupMsg)
		}
	case StateLeader:
		r.heartbeatElapsed += 1
		// check the heartbeatElapsed
		if r.heartbeatElapsed >= r.heartbeatTimeout{
			// Reset heartbeatElapsed
			r.resetHeartbeatElapsed()
			// Call Step function to handle a message type of MessageType_MsgBeat MessageType.
			localBeatMsg := r.newMsgBeat()
			r.Step(localBeatMsg)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// Reset Vote
	r.Vote = 0
	// Update term
	r.Term = term
	// Update leader
	r.Lead = lead
	// Update State
	r.State = StateFollower
	// Reset Vote
	r.resetVotes()
	// Reset electionElapsed and heartbeatElapsed
	r.resetElectionElapsed()
	r.resetHeartbeatElapsed()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// Increase term
	r.increaseTerm()
	// reset electionElapsed and heartbeatElapsed
	r.resetElectionElapsed()
	r.resetHeartbeatElapsed()
	// Get a random election timeout.
	r.electionTimeout = r.getRandomElectionTick()
	// Reset the vote pool
	r.resetVotes()
	// modify the State from follower to candidate
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// TODO: NOTE: Leader should propose a noop entry on its term
	// Update leader
	r.Lead = r.id
	// Reset electionElapsed and heartbeatElapsed
	r.resetElectionElapsed()
	r.resetHeartbeatElapsed()
	// Transfer state to StateLeader
	r.State = StateLeader
	// When a node become to candidate, it need send heartbeat to other peers.
	// Call Step function to handle a message type of MessageType_MsgBeat MessageType.
	localBeatMsg := r.newMsgBeat()
	r.Step(localBeatMsg)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		// The list of the type of the pb.Message m need handle when the state of
		// the peer r is StateFollower
		switch m.MsgType{
		case pb.MessageType_MsgHup:
			// Become Candidate
			r.becomeCandidate()
			// Send request vote to all the nodes in this cluster.
			r.sendAllRequestVotes()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}

	case StateCandidate:
		switch m.MsgType{
		case pb.MessageType_MsgHup:
			// Become Candidate
			// In becomeCandidate, it will increase the term of this peer, and set or reset some status of Raft,
			// such as electionElapsed and heartBeatElapsed.
			r.becomeCandidate()
			// Send request vote to all the nodes in this cluster.
			r.sendAllRequestVotes()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateLeader:
		switch m.MsgType{
		case pb.MessageType_MsgBeat:
			// Send heartbeat RPC to all the nodes in the cluster
			for id := range r.Prs{
				r.sendHeartbeat(id)
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// check Term, refer: raft_paper_test.go/TestCandidateFallback2AA
	if m.Term >= r.Term{
		// This raft peer is overdue, become follower
		r.becomeFollower(m.Term,m.From)
	}

}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message){
	resp := pb.Message{
		MsgType:              pb.MessageType_MsgRequestVoteResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 r.Term,			// set later
		LogTerm:              0,   				// TODO: modify here in 2AB log replication
		Index:                0,   				// TODO: modify here in 2AB log replication
		Entries:              nil, 				// TODO: modify here in 2AB log replication
		Commit:               0,   				// TODO: modify here in 2AB log replication
		Snapshot:             nil, 				// TODO: modify here in 2AC snapshot
		Reject:               true,				// set later
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	if m.Term < r.Term{
		resp.Reject = true
		// Append this response to r.msgs
		r.msgs = append(r.msgs,resp)
		return
	}
	// Now, I am sure about m.Term >= r.Term
	// Check the term
	if m.Term > r.Term{
		// become follower
		r.becomeFollower(m.Term, m.From)
	}
	// Check the VoteFor
	if r.Vote == 0 || r.Vote == m.From{
		// TODO: check the log (2AB)
		if true{
			// Vote
			r.Vote = m.From
			resp.Reject = false
		}
	}
	r.msgs = append(r.msgs,resp)
	return
}

// handleRequestVoteResponse handle RequestVote RPC response
func (r *Raft) handleRequestVoteResponse(m pb.Message){
	// check Term
	if m.Term > r.Term{
		// This raft peer is overdue, become follower
		r.becomeFollower(m.Term,m.From)
		return
	}
	// check vote
	if !m.Reject{
		r.votes[m.From] = true
	}
	// Count Votes
	if r.nVotes() >= (r.nPeers() / 2 + 1){
		// can be a leader
		r.becomeLeader()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// check Term
	if m.Term > r.Term{
		r.becomeFollower(m.Term,m.From)
		return
	}
	resp := pb.Message{
		MsgType:              pb.MessageType_MsgHeartbeatResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,   				// TODO: modify here in 2AB log replication
		Index:                0,   				// TODO: modify here in 2AB log replication
		Entries:              nil, 				// TODO: modify here in 2AB log replication
		Commit:               0,   				// TODO: modify here in 2AB log replication
		Snapshot:             nil, 				// TODO: modify here in 2AC snapshot
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	r.msgs = append(r.msgs,resp)
	// reset electionElapsed
	r.resetElectionElapsed()
}

// handleHeartbeatResponse handle Heartbeat RPC response
func (r *Raft) handleHeartbeatResponse(m pb.Message){
	if m.Term > r.Term{
		r.becomeFollower(m.Term,m.From)
		return
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) nVotes() uint64{
	var receiveVotes uint64 = 0
	for _,vote := range r.votes{
		if vote{
			receiveVotes += 1
		}
	}
	return receiveVotes
}

func (r *Raft) nPeers() uint64{
	return uint64(len(r.Prs))
}

func (r *Raft) increaseTerm(){
	r.Term += 1
}

func (r *Raft) resetElectionElapsed(){
	r.electionElapsed = 0
}

func (r *Raft) resetHeartbeatElapsed(){
	r.heartbeatElapsed = 0
}

func (r *Raft) resetVotes(){
	for id := range r.votes{
		r.votes[id] = false
	}
}

func (r *Raft) newMsgHup() pb.Message{
	return pb.Message{
		MsgType:              pb.MessageType_MsgHup,
		To:                   r.id,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,			// TODO: modify here in 2AB log replication
		Index:                0,			// TODO: modify here in 2AB log replication
		Entries:              nil,			// TODO: modify here in 2AB log replication
		Commit:               0,			// TODO: modify here in 2AB log replication
		Snapshot:             nil,			// TODO: modify here in 2AC snapshot
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
}

func (r *Raft) newMsgBeat() pb.Message{
	return pb.Message{
		MsgType:              pb.MessageType_MsgBeat,
		To:                   r.id,
		From:                 r.id,
		Term:                 r.Term,
		LogTerm:              0,			// TODO: modify here in 2AB log replication
		Index:                0,			// TODO: modify here in 2AB log replication
		Entries:              nil,			// TODO: modify here in 2AB log replication
		Commit:               0,			// TODO: modify here in 2AB log replication
		Snapshot:             nil,			// TODO: modify here in 2AC snapshot
		Reject:               false,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
}

func (r *Raft) getRandomElectionTick()int{
	return rand.Intn(10) + 10
}

func (r *Raft) sendAllRequestVotes(){
	// Vote for itself
	r.votes[r.id] = true
	r.Vote = r.id
	// If there is only one peer in this cluster, just become leader
	if r.nPeers() == 1{
		r.becomeLeader()
	}else{
		// Send requestVote RPC to all the nodes in the cluster.
		for id := range r.Prs{
			r.sendRequestVote(id)
		}
	}
}