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

	hadVotes map[uint64]bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	hs, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          nil,
		Prs:              nil,
		State:            StateFollower,
		votes:            nil,
		msgs:             nil,
		Lead:             0,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	r.RaftLog = newLog(c.Storage)
	r.msgs = make([]pb.Message, 0)
	r.Prs = make(map[uint64]*Progress)
	r.votes = make(map[uint64]bool)
	r.hadVotes = make(map[uint64]bool)
	for _, id := range c.peers {
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
		r.RaftLog.appliedTo(c.Applied)
	}
	rand.Seed(time.Now().Unix())
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.Prs[to].Next == r.RaftLog.LastIndex()+1 {
		return false
	}
	m := pb.Message{
		MsgType:  pb.MessageType_MsgAppend,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		LogTerm:  0,
		Index:    r.Prs[to].Next - 1,
		Entries:  nil,
		Commit:   r.RaftLog.committed,
		Snapshot: nil,
	}
	prevLogTerm, _ := r.RaftLog.Term(r.Prs[to].Next - 1)
	m.LogTerm = prevLogTerm
	m.Entries = make([]*pb.Entry, r.RaftLog.LastIndex()+1-r.Prs[to].Next)
	ents := r.RaftLog.getEns(r.Prs[to].Next, r.RaftLog.LastIndex()+1)
	for k := range ents {
		m.Entries[k] = &ents[k]
	}
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.id == to {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		To:      to,
		From:    r.id,
		Commit:  r.RaftLog.getCommited(),
		Reject:  false,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			localHubMsg := r.newHubMsg()
			r.Step(localHubMsg)
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			localHubMsg := r.newHubMsg()
			r.Step(localHubMsg)
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			localBeatMsg := r.newBeatMsg()
			r.Step(localBeatMsg)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.increaseTerm()
	r.resetElectionElapsed()
	r.resetHeartbeatElapsed()
	r.electionTimeout = r.getRandomElectionTick()
	r.resetVotes()
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.resetHeartbeatElapsed()
	r.resetElectionElapsed()
	r.State = StateLeader
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.bcastRequestVote()
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.Term = m.Term
			}
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			r.bcastRequestVote()
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			if !m.Reject {
				r.votes[r.id] = true
			}
		case pb.MessageType_MsgAppend:
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
			}
		case pb.MessageType_MsgPropose:
			for _, ent := range m.Entries {
				r.appendEntry(*ent)
			}
			if r.nPeers() == 1 {
				r.RaftLog.committed = r.RaftLog.LastIndex()
			} else {
				r.bcastAppend()
			}
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				r.sendHeartbeat(id)
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgAppend:
			{
				if m.Term >= r.Term {
					r.becomeFollower(m.Term, m.From)
				}
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	appendResp := pb.Message{
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
	defer func() {
		r.msgs = append(r.msgs, appendResp)
	}()
	if m.Term < r.Term {
		appendResp.Reject = true
		return
	}
	if match := r.RaftLog.tryMatch(m.Index, m.LogTerm); !match {
		appendResp.Reject = true
		return
	}
	ents := make([]*pb.Entry, len(m.Entries))
	copy(ents, m.Entries)
	for len(ents) != 0 {
		if match := r.RaftLog.tryMatch(ents[0].Index, ents[0].Term); !match {
			r.RaftLog.tryDeleteEnts(ents[0].Index - 1)
			break
		} else {
			ents = ents[1:]
		}
	}
	//TODO leader的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	if m.Commit > r.RaftLog.committed {
		if len(m.Entries) == 0 {
			r.RaftLog.committed = min(m.Index, m.Commit)
		} else {
			r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
		}
	}
	appendResp.Commit = r.RaftLog.getCommited()
	appendResp.Index = r.RaftLog.LastIndex()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	leaderTerm := m.Term
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
	}
	if r.Term > leaderTerm {
		msg.Reject = true
	}
	msg.Reject = false
	r.msgs = append(r.msgs, msg)
}

// handle RequestVote
func (r *Raft) handleRequestVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}
	if m.Term < r.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	//TODO: *****
	if r.Vote == 0 || r.Vote == m.From {
		lt, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		if lt < m.LogTerm {
			r.Vote = m.From
			msg.Reject = false
		} else if lt == m.LogTerm {
			if r.RaftLog.LastIndex() <= m.Index {
				r.Vote = m.From
				msg.Reject = false
			}
		}
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	candidateId := m.From
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      candidateId,
		From:    r.id,
		Term:    r.Term,
	}

	if r.Vote == None && m.Term >= r.Term {
		r.Vote = candidateId
		msg.Reject = false
	} else if r.Vote == m.From && m.Term >= r.Term {
		r.Vote = candidateId
		msg.Reject = false
	} else {
		msg.Reject = true
	}
	r.msgs = append(r.msgs, msg)
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

func getNodesId(r *Raft) []uint64 {
	nodes := make([]uint64, 0, len(r.Prs))
	for id := range r.Prs {
		nodes = append(nodes, id)
	}
	return nodes
}

func (r *Raft) increaseTerm() {
	r.Term++
}

func (r *Raft) newHubMsg() (m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHup,
		Term:    r.Term,
		From:    r.id,
	}
	return msg
}

func (r *Raft) newBeatMsg() (m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		Term:    r.Term,
		From:    r.id,
	}
	return msg
}

func (r *Raft) newTimeOutMSg() (m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		Term:    r.Term,
		From:    r.id,
	}
	return msg
}

func (r *Raft) loadState(hs pb.HardState) {
	if hs.Commit < r.RaftLog.getCommitted() || hs.Commit > r.RaftLog.LastIndex() {
		panic("In `Raft.loadState`: the commit of hardState out of range.")
	}
	//当进行newLog的时候已经赋值了一遍？？
	r.RaftLog.committed = hs.Commit
	r.Term = hs.Term
	r.Vote = hs.Vote
}

func (r *Raft) appendEntry(ents ...pb.Entry) {
	li := r.RaftLog.LastIndex()
	switch r.State {
	case StateLeader:
		for k, _ := range ents {
			ents[k].Index = li + uint64(k) + 1
			ents[k].Term = r.Term
		}
		r.RaftLog.append(ents...)
		r.Prs[r.id].Next += uint64(len(ents))
		r.Prs[r.id].Match += uint64(len(ents))
	case StateCandidate:
		r.RaftLog.append(ents...)
	case StateFollower:
		r.RaftLog.append(ents...)
	}
}

func (r *Raft) nPeers() uint64 {
	return uint64(len(r.Prs))
}

func (r *Raft) bcastAppend() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		return
	}
	if m.Reject {
		r.Prs[m.From].Next -= 1
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Next = m.Index + 1
	r.Prs[m.From].Match = r.Prs[m.From].Next - 1
	lt, _ := r.RaftLog.Term(r.Prs[m.From].Next - 1)
	if r.Term == lt {
		if r.nCopied(r.Prs[m.From].Match) >= r.nPeers()/2+1 && r.RaftLog.getCommited() < r.Prs[m.From].Match {
			updateCommit := r.RaftLog.tryCommit(r.Prs[m.From].Match)
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
						Index:   r.Prs[r.id].Next - 1,
						Entries: nil,
						Commit:  r.RaftLog.getCommited(),
					}
					lt, _ = r.RaftLog.Term(r.Prs[id].Next - 1)
					cm.LogTerm = lt
					r.msgs = append(r.msgs, cm)
				}
			}
		}
	}
}

func (r *Raft) nCopied(i uint64) uint64 {
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

func (r *Raft) resetElectionElapsed() {
	r.electionElapsed = 0
}

func (r *Raft) resetHeartbeatElapsed() {
	r.heartbeatElapsed = 0
}

func (r *Raft) getRandomElectionTick() int {
	return rand.Intn(10) + 10
}

func (r *Raft) resetVotes() {
	for id := range r.votes {
		r.votes[id] = false
		r.hadVotes[id] = false
	}
}

func (r *Raft) bcastRequestVote() {
	r.votes[r.id] = true
	r.hadVotes[r.id] = true
	r.Vote = r.id
	if r.nPeers() == 1 {
		r.becomeLeader()
	} else {
		for id := range r.Prs {
			r.sendRequestVote(id)
		}
	}
}

func (r *Raft) sendRequestVote(to uint64) {
	if r.id == to {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgRequestVote,
		To:       to,
		From:     r.id,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   false,
	}
	msg.Index = r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	msg.LogTerm = term
	r.msgs = append(r.msgs, msg)
}
