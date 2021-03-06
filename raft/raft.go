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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
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

var ErrInTransferLeader = errors.New("in transferee leader")

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

// Progress represents a follower???s progress in the view of the leader. Leader maintains
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
	// Add in 2aa:
	// To record if the follower votes in this term
	hadVotes map[uint64]bool

	pendingElectionElapsed int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// get the hard state and the confState
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	// Your Code Here (2A).
	r := &Raft{
		id:   c.ID,
		Term: 0, // In raft paper, the term of a new raft peer is 0.
		Vote: 0, // the config guarantee the id will greater than 0,
		// so the Vote set to 0 can represent that there is
		// no vote in the current state of this node.
		RaftLog:          nil,             // TODO: how to set RaftLog
		Prs:              nil,             // TODO: set later, (2AB)
		State:            StateFollower,   // In raft paper, the init state is candidate.
		votes:            nil,             // set later
		msgs:             nil,             // TODO: how to set
		Lead:             0,               // In raft paper, a new raft peer will have no leader
		heartbeatTimeout: c.HeartbeatTick, // TODO: check
		electionTimeout:  c.ElectionTick,  // TODO: check
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0, // TODO: 3A leader transfer
		PendingConfIndex: 0, // TODO: 3A conf change
	}
	r.RaftLog = newLog(c.Storage)
	r.msgs = make([]pb.Message, 0)
	r.votes = make(map[uint64]bool)
	r.hadVotes = make(map[uint64]bool)
	r.Prs = make(map[uint64]*Progress)
	// The id in c.peers just used in test
	for _, id := range c.peers {
		r.votes[id] = false
		r.hadVotes[id] = false
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	// Read the true id from confstate.
	for _, id := range cs.Nodes {
		r.votes[id] = false
		r.hadVotes[id] = false
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	// Set random seed
	rand.Seed(time.Now().Unix())
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// There are several situations where you don't need to send rpc:
	// 1. The log of `to` is as new as leader.
	if to == r.id {
		return false
	}
	if r.Prs[to].Next == r.RaftLog.LastIndex()+1 {
		return false
	}
	m := pb.Message{
		MsgType:  pb.MessageType_MsgAppend,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,
		Index:    r.Prs[to].Next - 1,
		Entries:  nil,
		Commit:   r.RaftLog.committed,
		Snapshot: nil,
	}
	// m.LogTerm is prevLogTerm in raft paper
	// TODO: Modify here if you want to handle the error.
	lt, err := r.RaftLog.Term(r.Prs[to].Next - 1)

	if err == ErrCompacted {
		r.sendSnapshot(to)
		return true
	} else if err != nil {
		log.Panic(err)
	}
	m.LogTerm = lt
	if r.RaftLog.LastIndex()+1 < r.Prs[to].Next {
		log.Panicf("Peer %d : r.RaftLog.LastIndex() + 1 = %d, but r.Prs[%d].Next = %d", r.id, r.RaftLog.LastIndex()+1, to, r.Prs[to].Next)
	}
	m.Entries = make([]*pb.Entry, 0, r.RaftLog.LastIndex()+1-r.Prs[to].Next)
	ents := r.RaftLog.getEntsInMem(r.Prs[to].Next, r.RaftLog.LastIndex()+1)
	for k := range ents {
		m.Entries = append(m.Entries, &ents[k])
		//m.Entries[k] = &ents[k]
	}
	r.msgs = append(r.msgs, m)
	return true
}

func (r *Raft) sendSnapshot(to uint64) {
	if to == r.id {
		return
	}
	snapshotMsg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   false,
	}
	s := r.RaftLog.getSnapshot()
	if s == nil {
		// send snapshot later
		return
	}
	snapshotMsg.Snapshot = s
	log.Infof("Raft:%d send snapshot to %d", r.id, to)
	r.msgs = append(r.msgs, snapshotMsg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if to == r.id {
		return
	}
	// send heart beat
	heartbeatMessage := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		// A peer will not handle the prevTerm and prevIndex in HeartBeatMsg.
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil, // TODO: modify here in 2AC snapshot
		Reject:   false,
	}
	// Only commit this term entry by calculate backup.
	if t, err := r.RaftLog.Term(r.RaftLog.getCommited()); err != nil && t == r.Term {
		heartbeatMessage.Commit = r.RaftLog.getCommited()
	}
	r.msgs = append(r.msgs, heartbeatMessage)
}

// sendRequestVote sends a requestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	if to == r.id {
		return
	}
	reqVoteMessage := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: 0,
		Index:   0,
		// Vote msg will not carry the entries.
		Entries:  nil,
		Commit:   0,
		Snapshot: nil, // TODO: modify here in 2AC snapshot
		Reject:   false,
	}
	// In the message type of MsgRequestVote, the LogTerm is the last term, the Index is the last index
	reqVoteMessage.Index = r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	reqVoteMessage.LogTerm = term
	r.msgs = append(r.msgs, reqVoteMessage)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if r.pendingElectionElapsed < 0 {
			r.pendingElectionElapsed++
		} else {
			r.electionElapsed += 1
		}
		// check the electionElapsed
		if r.electionElapsed >= r.electionTimeout {
			// If this peer had not received the heartbeat during the r.electionTimeout,
			// need become candidate and prepare to start a election
			// Call Step function to handle a message type of MessageType_MsgHup MessageType.
			localHupMsg := r.newMsgHup()
			r.Step(localHupMsg)
		}
	case StateCandidate:
		if r.pendingElectionElapsed < 0 {
			r.pendingElectionElapsed++
		} else {
			r.electionElapsed += 1
		}
		if r.electionElapsed >= r.electionTimeout {
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
		if r.heartbeatElapsed >= r.heartbeatTimeout {
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
	// Reset leaderTransferee
	r.leadTransferee = None
	// Reset VoteFor
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
	log.Infof("Peer %d Become Candidate in term %d.", r.id, r.Term)
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
	log.Infof("Peer %d Become Leader in term %d.", r.id, r.Term)
	r.Lead = r.id
	// Reset electionElapsed and heartbeatElapsed
	r.resetElectionElapsed()
	r.resetHeartbeatElapsed()
	// Transfer state to StateLeader
	r.State = StateLeader
	// In raft paper, a peer should send heartbeat when it becomes leader,
	// but in TestLeaderStartReplication2AB, it will not hope get heartbeat msg.
	// When a node become to candidate, it need send heartbeat to other peers.
	// Call Step function to handle a message type of MessageType_MsgBeat MessageType.
	//localBeatMsg := r.newMsgBeat()
	//r.Step(localBeatMsg)
	// reset r.Prs
	for id, _ := range r.Prs {
		if id == r.id {
			r.Prs[id].Next = r.RaftLog.LastIndex() + 1
			r.Prs[id].Match = r.RaftLog.LastIndex()
		} else {
			r.Prs[id].Next = r.RaftLog.LastIndex() + 1
			r.Prs[id].Match = 0
		}
	}
	// Leader should append a noop entry on it's term
	emptyEntry := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Data:      nil,
	}
	// leader append a noop entry and send propose
	r.appendEntry(emptyEntry)
	if r.nPeers() == 1 {
		r.RaftLog.tryCommit(r.RaftLog.LastIndex())
	} else {
		r.bcastAppend()
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		// The list of the type of the pb.Message m need handle when the state of
		// the peer r is StateFollower
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
		case pb.MessageType_MsgHup:
			// Become Candidate
			r.becomeCandidate()
			// Send request vote to all the nodes in this cluster.
			r.bcastRequestVote()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow(m)
		case pb.MessageType_MsgTransferLeader:
			r.processLeaderTransfer(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// Become Candidate
			// In becomeCandidate, it will increase the term of this peer, and set or reset some status of Raft,
			// such as electionElapsed and heartBeatElapsed.
			r.becomeCandidate()
			// Send request vote to all the nodes in this cluster.
			r.bcastRequestVote()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.handleTimeoutNow(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			// Send heartbeat RPC to all the nodes in the cluster
			for id := range r.Prs {
				r.sendHeartbeat(id)
			}
		case pb.MessageType_MsgPropose:
			if r.leadTransferee != None {
				// Stop receive propose
				return ErrInTransferLeader
			}
			for _, ent := range m.Entries {
				//log.Infof("Peer %d receive a propose, index is %d, data is : %v", r.id, r.RaftLog.LastIndex()+1, ent.Data)
				r.appendEntry(*ent)
			}
			if r.nPeers() == 1 {
				// commit
				r.RaftLog.committed = r.RaftLog.LastIndex()
			} else {
				r.bcastAppend()
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			r.processLeaderTransfer(m)
		}
	}
	return nil
}

func (r *Raft) processLeaderTransfer(m pb.Message) {
	if r.id == m.From && r.State == StateLeader {
		return
	}
	switch r.State {
	case StateLeader:
		// check the transferee in prs?
		if r.Prs[m.From] == nil {
			return
		}
		r.leadTransferee = m.From
		// Send Append to transferee
		if !r.sendAppend(r.leadTransferee) {
			// up to date
			r.sendTimeoutNow(m.From)
		}
	case StateFollower:
		// new election
		r.Step(r.newMsgHup())
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// check Term, refer: raft_paper_test.go/TestCandidateFallback2AA
	if m.Term >= r.Term {
		// This raft peer is overdue, become follower
		r.becomeFollower(m.Term, m.From)
	}
	appendResp := pb.Message{
		MsgType:  pb.MessageType_MsgAppendResponse,
		To:       m.From,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  m.LogTerm,
		Index:    m.Index,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   false,
	}
	defer func() {
		if !appendResp.Reject {
			r.Lead = m.From
		}
		r.msgs = append(r.msgs, appendResp)
	}()
	// Reject if sender is overdue of term.
	if m.Term < r.Term {
		appendResp.Reject = true
		return
	}
	// Try to match the prevIndex and prevTerm
	if match := r.RaftLog.tryMatch(m.Index, m.LogTerm); !match {
		log.Infof("Raft %d try match (%d,%d), but false, so reject.", r.id, m.Index, m.LogTerm)
		// Reject if log don't match, resp.Index and resp.Term are same as the m
		appendResp.Reject = true
		// Quick Match, return the first Index of corresponding Term
		//if len(r.RaftLog.entries) != 0 {
		//	for i := 0; i < len(r.RaftLog.entries); i++ {
		//		if r.RaftLog.entries[i].Term == m.Term {
		//			appendResp.Entries = []*pb.Entry{
		//				&(r.RaftLog.entries[i]),
		//			}
		//		}
		//		break
		//	}
		//}
		return
	}
	// append the entry in my log
	// Record the new appendEntry
	lastNewEntryIndex := m.Index
	for len(m.Entries) != 0 {
		entp := m.Entries[0]
		if match := r.RaftLog.tryMatch(entp.Index, entp.Term); !match {
			// If it doesn't match, delete it and all logs after it
			r.RaftLog.tryDeleteEntsAfterIndex(entp.Index - 1)
			break
		} else {
			lastNewEntryIndex = entp.Index
			m.Entries = m.Entries[1:]
		}
	}
	if len(m.Entries) != 0 {
		//log.Infof("Raft %d receive log from peer %d range [%d,%d]", r.id, m.From, m.Entries[0].Index, m.Entries[len(m.Entries)-1].Index)
	}
	for _, entp := range m.Entries {
		lastNewEntryIndex = entp.Index
		r.appendEntry(*entp)
	}
	// update the commit index
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// Why need compare the leaderCommit and the commitIndex ? Suppose a scenario:
	// Three nodes in the raft cluster, and they have the same log, and node 1 is leader.
	// The commit is 6, 6, 5 in the node 1, 2, 3, because the network of node 3 is congested
	// and the msg carrying the commit index 6 had loss.
	// Node 1 collapses before it send the newest commit 6 to the node3.
	// Node3 starts an election and win it(because node3 and node2 have the same log), and send a append msg with
	// commit = 5. We don't hope node 2 decrease the commit index.

	// Or you can only using r.RaftLog.tryCommit, because tryCommit will do nothing if the index <= i
	if m.Commit > r.RaftLog.getCommited() {
		r.RaftLog.tryCommit(min(lastNewEntryIndex, m.Commit))
	}
	appendResp.Commit = r.RaftLog.getCommited()
	appendResp.Index = lastNewEntryIndex
	t, _ := r.RaftLog.Term(appendResp.Index)
	appendResp.LogTerm = t
	r.resetElectionElapsed()
	return
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	if m.Term < r.Term {
		return
	}
	// If m.From reject append RPC because of don't match the prevLog, decrease the r.Prs[to].Next
	if m.Reject && m.Index == r.Prs[m.From].Next-1 {
		// Find the first index of return term2
		r.Prs[m.From].Next -= 1
		//if len(m.Entries) != 0 {
		//	ent := m.Entries[0]
		//	for i := 0; i < len(r.RaftLog.entries); i++ {
		//		if ent.Term == r.RaftLog.entries[i].Term {
		//			if r.Prs[m.From].Next != r.RaftLog.entries[i].Index {
		//				r.Prs[m.From].Next = r.RaftLog.entries[i].Index
		//			}else {
		//				r.Prs[m.From].Next--
		//			}
		//			break
		//		}
		//	}
		//} else {
		//	r.Prs[m.From].Next -= 1
		//}
		// if reject, need send again with new next
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Next = m.Index + 1
	r.Prs[m.From].Match = m.Index
	lt, _ := r.RaftLog.Term(r.Prs[m.From].Next - 1)
	// The leader only commits the logs for the current term by calculating copies
	if r.Term != lt {
		return
	}
	if r.nCopied(r.Prs[m.From].Match) >= r.nPeers()/2+1 && r.RaftLog.getCommited() < r.Prs[m.From].Match {
		updateCommit := r.RaftLog.tryCommit(r.Prs[m.From].Match)
		// Tell all followers the newest commit index, it will only send once per peer, but it is safe
		if updateCommit {
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				cm := pb.Message{
					MsgType: pb.MessageType_MsgAppend,
					To:      id,
					From:    r.id,
					Term:    r.Term,
					LogTerm: 0,
					Index:   r.Prs[m.From].Match,
					Entries: nil,
					Commit:  r.RaftLog.getCommited(),
				}
				lt, _ = r.RaftLog.Term(r.Prs[r.id].Match)
				cm.LogTerm = lt
				r.msgs = append(r.msgs, cm)
			}
		}
	}
	if r.Prs[m.From].Next == r.RaftLog.LastIndex()+1 && m.From == r.leadTransferee {
		// send time out now
		r.sendTimeoutNow(m.From)
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	resp := pb.Message{
		MsgType:  pb.MessageType_MsgRequestVoteResponse,
		To:       m.From,
		From:     r.id,
		Term:     r.Term, // set later
		LogTerm:  0,      // TODO: modify here in 2AB log replication
		Index:    0,      // TODO: modify here in 2AB log replication
		Entries:  nil,    // TODO: modify here in 2AB log replication
		Commit:   0,      // TODO: modify here in 2AB log replication
		Snapshot: nil,    // TODO: modify here in 2AC snapshot
		Reject:   true,   // set later
	}
	if m.Term < r.Term {
		resp.Reject = true
		// Append this response to r.msgs
		r.msgs = append(r.msgs, resp)
		return
	}
	// Now, I am sure about m.Term >= r.Term
	// Check the term
	if m.Term > r.Term {
		// become follower
		r.becomeFollower(m.Term, None)
	}
	// Check the VoteFor
	if r.Vote == 0 || r.Vote == m.From {
		lt, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		// compare the last term
		if lt < m.LogTerm {
			// Vote
			r.Vote = m.From
			r.resetElectionElapsed()
			resp.Reject = false
		} else if lt == m.LogTerm {
			if r.RaftLog.LastIndex() <= m.Index {
				// Vote
				r.Vote = m.From
				resp.Reject = false
				r.resetElectionElapsed()
			}
		}
	}
	r.msgs = append(r.msgs, resp)
	return
}

// bcastAppend send a message type of append to append the ents to other peers
func (r *Raft) bcastAppend() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// handleRequestVoteResponse handle RequestVote RPC response
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// check Term
	if m.Term > r.Term {
		// This raft peer is overdue, become follower
		r.becomeFollower(m.Term, m.From)
		return
	}
	r.hadVotes[m.From] = true
	// check vote
	if !m.Reject {
		r.votes[m.From] = true
	}
	// Count Agree Votes
	if r.nAgreeVotes() >= (r.nPeers()/2 + 1) {
		// can be a leader
		r.becomeLeader()
	}
	// Count Reject Votes
	// If program goes there, in this cluster, this peer's log is newest as quorum of nodes,
	// but they had voted for other or have the newest log, transfer to follower with origin term
	// and unknown leader.
	if r.nRejectVotes() >= (r.nPeers()/2 + 1) {
		// transfer to follower
		r.becomeFollower(r.Term, None)
		if r.pendingElectionElapsed != -2{
			r.pendingElectionElapsed= -2
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// check Term
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	r.Lead = m.From
	resp := pb.Message{
		MsgType:  pb.MessageType_MsgHeartbeatResponse,
		To:       m.From,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   false,
	}
	r.RaftLog.tryCommit(m.Commit)
	resp.Commit = r.RaftLog.getCommited()
	resp.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, resp)
	// reset electionElapsed
	r.resetElectionElapsed()
}

// handleHeartbeatResponse handle Heartbeat RPC response
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	// Leader can send log to followers when it received a heartbeat response
	// which indicate it doesn't have update-to-date log
	li := r.RaftLog.LastIndex()
	lt, _ := r.RaftLog.Term(li)
	if m.LogTerm != lt || m.Index != li {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	meta := m.Snapshot.Metadata
	log.Infof("Raft %d receive snapshot from %d, meta: %d,%d", r.id, m.From, meta.Index, meta.Term)
	resp := pb.Message{
		MsgType:  pb.MessageType_MsgAppendResponse,
		To:       m.From,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   false,
	}
	resp.Index = meta.Index
	resp.LogTerm = meta.Term
	defer func() {
		r.msgs = append(r.msgs, resp)
	}()
	if m.Term < r.Term {
		resp.Reject = true
		return
	}
	// Avoid commit rollback
	if meta.Index <= r.RaftLog.committed {
		// don't do anything
		return
	}
	if match := r.RaftLog.tryMatch(meta.Index, meta.Term); match {
		// delete entries before meta.index
		r.RaftLog.tryDeleteEntsBeforeIndex(meta.Index)
	} else {
		// Clean all log
		r.RaftLog.entries = make([]pb.Entry, 0)
	}
	//log.Infof("Raft %d receive snapshot from %d", r.id, m.From)
	// pending it
	r.RaftLog.pendingSnapshot = m.Snapshot
	// update some status
	r.RaftLog.applied = meta.Index
	r.RaftLog.committed = meta.Index
	r.RaftLog.stabled = meta.Index
	for _, id := range meta.ConfState.Nodes {
		r.Prs[id] = &Progress{}
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  2,
		}
		log.Infof("Raft %d add node %d, now node list: %v", r.id, id, func() []uint64 {
			res := make(uint64Slice, 0)
			for pid := range r.Prs {
				res = append(res, pid)
			}
			sort.Sort(res)
			return res
		}())
		r.votes[id] = false
		r.hadVotes[id] = false
	}
	r.PendingConfIndex = None
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		delete(r.votes, id)
		delete(r.hadVotes, id)
		log.Infof("Raft %d remove node %d, now node list: %v", r.id, id, func() []uint64 {
			res := make(uint64Slice, 0)
			for pid := range r.Prs {
				res = append(res, pid)
			}
			sort.Sort(res)
			return res
		}())
		if r.State == StateLeader {
			// Try to update commit
			match := make(uint64Slice, len(r.Prs))
			i := 0
			for _, prs := range r.Prs {
				match[i] = prs.Match
				i += 1
			}
			sort.Sort(match)
			// n is the new committed
			n := match[(len(r.Prs)-1)/2]
			if n > r.RaftLog.getCommited() {
				t, err := r.RaftLog.Term(n)
				if err != nil {
					log.Panic(err)
				}
				// Only update the current term entry by calculating the backups
				if t == r.Term {
					r.RaftLog.committed = n
					for to := range r.Prs {
						r.sendHeartbeat(to)
					}
				}
			}
		}
	}
	r.PendingConfIndex = None
}

func (r *Raft) nAgreeVotes() uint64 {
	var receiveAgreeVotes uint64 = 0
	for k, _ := range r.votes {
		if r.votes[k] && r.hadVotes[k] {
			receiveAgreeVotes += 1
		}
	}
	return receiveAgreeVotes
}

func (r *Raft) nRejectVotes() uint64 {
	var receiveRejectVotes uint64 = 0
	for k, _ := range r.votes {
		if (!r.votes[k]) && r.hadVotes[k] {
			receiveRejectVotes += 1
		}
	}
	return receiveRejectVotes
}

func (r *Raft) nPeers() uint64 {
	return uint64(len(r.Prs))
}

// increaseTerm will do term+=1
func (r *Raft) increaseTerm() {
	r.Term += 1
}

func (r *Raft) resetElectionElapsed() {
	r.electionElapsed = 0
}

func (r *Raft) resetHeartbeatElapsed() {
	r.heartbeatElapsed = 0
}

func (r *Raft) resetVotes() {
	for id := range r.votes {
		r.votes[id] = false
		r.hadVotes[id] = false
	}
}

func (r *Raft) newMsgHup() pb.Message {
	return pb.Message{
		MsgType:  pb.MessageType_MsgHup,
		To:       r.id,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   false,
	}
}

func (r *Raft) newMsgBeat() pb.Message {
	return pb.Message{
		MsgType:  pb.MessageType_MsgBeat,
		To:       r.id,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,   // TODO: modify here in 2AB log replication
		Index:    0,   // TODO: modify here in 2AB log replication
		Entries:  nil, // TODO: modify here in 2AB log replication
		Commit:   0,   // TODO: modify here in 2AB log replication
		Snapshot: nil, // TODO: modify here in 2AC snapshot
		Reject:   false,
	}
}

func (r *Raft) getRandomElectionTick() int {
	return rand.Intn(10) + 10
}

func (r *Raft) bcastRequestVote() {
	// Vote for itself
	r.votes[r.id] = true
	r.hadVotes[r.id] = true
	r.Vote = r.id
	// If there is only one peer in this cluster, just become leader
	if r.nPeers() == 1 {
		r.becomeLeader()
	} else {
		// Send requestVote RPC to all the nodes in the cluster.
		for id := range r.Prs {
			r.sendRequestVote(id)
		}
	}
}
func (r *Raft) appendEntry(ents ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	switch r.State {
	case StateLeader:
		// If the state is leader, need set the index and the term
		for k, _ := range ents {
			ents[k].Index = li + uint64(k) + 1
			ents[k].Term = r.Term
			if ents[k].EntryType == pb.EntryType_EntryConfChange {
				if r.PendingConfIndex != None {
					continue
				} else {
					r.PendingConfIndex = ents[k].Index
				}
			}
		}
		r.RaftLog.append(ents...)
		// Update the match and the next
		r.Prs[r.id].Next += uint64(len(ents))
		r.Prs[r.id].Match += uint64(len(ents))
	case StateCandidate:
		r.RaftLog.append(ents...)
	case StateFollower:
		r.RaftLog.append(ents...)
	}
}

func (r *Raft) nCopied(i uint64) uint64 {
	// n will be init as 1, because it will at least occurs in r
	var n uint64 = 1
	for id, p := range r.Prs {
		if id == r.id {
			continue
		}
		if p.Match >= i {
			n += 1
		}
	}
	return n
}

func (r *Raft) loadState(hs pb.HardState) {
	if hs.Commit < r.RaftLog.getCommited() || hs.Commit > r.RaftLog.LastIndex() {
		panic("In `Raft.loadState`: the commit of hardState out of range.")
	}
	r.RaftLog.committed = hs.Commit
	r.Term = hs.Term
	r.Vote = hs.Vote
}

// getMsgs get the msg from r.msgs, and return them to upper applications.
func (r *Raft) getMsgs() []pb.Message {
	if len(r.msgs) == 0 {
		return nil
	}
	msgs := make([]pb.Message, 0, len(r.msgs))
	rmsgs := make([]pb.Message, 0, len(r.msgs))
	for _, msg := range r.msgs {
		if msg.To == r.id {
			rmsgs = append(rmsgs, msg)
		} else {
			msgs = append(msgs, msg)
		}
	}
	r.msgs = rmsgs
	return msgs
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.getCommited(),
	}
}

func (r *Raft) advance(rd Ready) {
	// TODO: Check here in part 2c
	// handle the applied
	if newApplied := rd.appliedCursor(); newApplied > 0 {
		r.RaftLog.appliedTo(newApplied)
	}
	// TODO: handle the conf change in multi-raft or conf change
	if len(rd.Entries) > 0 {
		r.RaftLog.stableTo(rd.Entries[len(rd.Entries)-1].Index)
	}
	// TODO: handle the snapshot
}

func (r *Raft) sendTimeoutNow(to uint64) {
	if to == r.id {
		return
	}
	// check up to date
	if r.Prs[to].Next != r.RaftLog.LastIndex()+1 {
		log.Panicf("Transferee not up to date")
	}
	m := pb.Message{
		MsgType:  pb.MessageType_MsgTimeoutNow,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   false,
	}
	prevTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	m.LogTerm = prevTerm
	m.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, m)
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	if m.Term != r.Term {
		return
	}
	if !r.checkIdInRaftGroup(r.id) {
		return
	}
	if match := r.RaftLog.tryMatch(m.Index, m.LogTerm); match {
		r.Step(r.newMsgHup())
	}
	r.leadTransferee = None
}

func (r *Raft) checkIdInRaftGroup(id uint64) bool {
	if p := r.Prs[id]; p != nil {
		return true
	}
	return false
}
