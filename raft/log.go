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
)

// EntriesUnavailable is returned by RaftLog interface when the requested log entries
// are unavailable.
var EntriesUnavailable = errors.New("[RaftLog]: Requested entry at index is unavailable")

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// If a peer was start with begin status, the entries will like this:
// Index of RaftLog.Entries:		  		| 0		1		2		...		n	|
// 								      		+-------------------------------------+
// Index of Entry in RaftLog.Entries: 	| 1	    2		3		...		n+1	|
// If a peer was restart with a conf, the entries will like this : (offset = entries[0].Index)
// Index of RaftLog.Entries:		  | 0				1		...			n		|
// 								      +---------------------------------------------+
// Index of Entry in RaftLog.Entries: | offset 		offset+1	...		offset+n	|
// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	// The application need protect that the storage can not be nil.
	// To new a storage by using: ms := NewMemoryStorage()
	if storage == nil {
		panic("Storage cannot be nil.")
	}
	rl := &RaftLog{
		storage:         storage,
		committed:       0, // set later
		applied:         0,
		stabled:         0,
		entries:         nil,
		pendingSnapshot: nil, // TODO: how to set
	}
	// Read the commit index from HardState and set the commit of `rl`.
	// err will always be nil, refer: MemoryStorage.InitialState
	// TODO: storage.InitialState will return a confState, modify here if you want to handle it.
	hs, _, isErr := storage.InitialState()
	if isErr != nil {
		panic(isErr)
	}
	rl.committed = hs.Commit
	// Read the applied index from snapshot in storage
	// sErr is always be nil in the MemoryStorage.
	snapshot, sErr := storage.Snapshot()
	if sErr != nil {
		panic(sErr)
	}
	rl.applied = snapshot.Metadata.Index
	// Get the firstIndex and lastIndex from storage.
	// TODO: check here if you modify the log logic.
	fi, fiErr := storage.FirstIndex()
	if fiErr != nil {
		panic(fiErr)
	}
	li, liErr := storage.LastIndex()
	if liErr != nil {
		panic(liErr)
	}
	if fi == li+1 {
		// The storage only contain a dummy entry or an entry which records the compact index.
		rl.entries = make([]pb.Entry, 0)
	} else {
		// TODO: Modify the _ when you want to handle the Error
		ents, _ := storage.Entries(fi, li+1)
		// TODO: Instead of copy?
		rl.entries = ents
	}
	// set the stabled
	rl.stabled = li
	return rl
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.isEntriesEmpty() {
		// There is no entry in the raftLog
		return []pb.Entry{}
	}
	if l.stabled == l.LastIndex() {
		// There is no unstable entry in the raftLog
		return []pb.Entry{}
	}
	// Get the offset of the entry
	offset := l.entries[0].Index
	ents := make([]pb.Entry, l.LastIndex()-l.stabled)
	for k, ent := range l.entries[l.stabled-offset+1:] {
		ents[k] = ent
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l.isEntriesEmpty() {
		// There is no entry in the raftLog
		return []pb.Entry{}
	}
	if l.committed == l.applied {
		// There is no had committed but not applied entry in the raftLog
		return []pb.Entry{}
	}
	offset := l.entries[0].Index
	ents = make([]pb.Entry, l.committed-l.applied)
	for k, ent := range l.entries[l.applied-offset+1 : l.committed-offset+1] {
		ents[k] = ent
	}
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if l.isEntriesEmpty() {
		return l.stabled
	}
	// l.entries[len(l.entries)-1].Index will always greater or equal than the l.stabled.
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// We don't hope try to get the term of index 0.
	// There will an situation that will call Term(0):
	// 1. A raft cluster is be init as start status(no snapshot and no storage)
	// a node win the first election, and append a noop entry, set the Prs[otherNodeId].Next = 1,
	// and it will send append msg and set the prevTerm by calling Term(Prs[to].Next-1) = Term(0)
	if i == 0 {
		return 0, nil
	}
	// There will be three situations that will cause the log entries to be empty:
	// 1. A raft system is just init as start status(no snapshot and no storage)
	// and all nodes are at term 0(None leader election happened).
	// 2. A raft system is at term 1, but follower had not received log(even the noop entry).
	// 3. All stabled entries are compacted.
	if l.isEntriesEmpty() {
		return 0, EntriesUnavailable
	}
	offset := l.entries[0].Index
	if i > l.LastIndex() || i < offset {
		// If the index is greater than the LastIndex or try to get a term which had been compacted,
		// it will return an error
		return 0, EntriesUnavailable
	}
	return l.entries[i-offset].Term, nil
}

// isEntriesEmpty return true if the len(l.entries) == 0
func (l *RaftLog) isEntriesEmpty() bool {
	return len(l.entries) == 0
}

// append will append the ents into the entries
func (l *RaftLog) append(ents ...pb.Entry) {
	if len(ents) == 0 {
		return
	}
	l.entries = append(l.entries, ents...)
}

// getEnts returns the index of entries in range [lo, hi)
func (l *RaftLog) getEnts(lo uint64, hi uint64) []pb.Entry {
	if l.isEntriesEmpty() {
		panic("Entries of RaftLog is empty.")
	}
	offset := l.entries[0].Index
	// May be a erroneous request or try to get a entry which had been compacted.
	if lo < offset || hi > l.LastIndex()+1 {
		log.Panicf("Try get ents [%d:%d] out of range [%d:%d].", lo, hi, offset, l.LastIndex())
	}
	ents := make([]pb.Entry, hi-lo)
	for k, ent := range l.entries[lo-offset : hi-offset] {
		ents[k] = ent
	}
	return ents
}

// Try commit will try to commit the log entries whose index <= i
// It will do nothing if i <= committed, and commit to the min(l.LastIndex(), i)
//
func (l *RaftLog) tryCommit(i uint64) bool {
	if i <= l.committed {
		return false
	}
	l.committed = min(i, l.LastIndex())
	return true
}

func (l *RaftLog) getCommited() uint64 {
	return l.committed
}

// tryMatch try to find a log which index is i and term is t
func (l *RaftLog) tryMatch(i, t uint64) bool {
	// tryMatch(0) will always return true.
	if i == 0 {
		return true
	}
	// Try to match a log which had been compacted.
	if l.isEntriesEmpty() && i <= l.stabled {
		// TODO: return false instead?
		log.Panicf("Try to match (i,t) = (%d,%d) in the raftLog which stabled = %d and entries is empty.", i, t, l.stabled)
	}
	// Try to match a log that has not been received
	if i > l.LastIndex() {
		return false
	}
	offset := l.entries[0].Index
	return (l.entries[i-offset].Term) == t
}

// tryDeleteEnts will try to delete the entries which index > i
func (l *RaftLog) tryDeleteEnts(i uint64) {
	// No entries can be delete.
	if l.isEntriesEmpty() {
		return
	}
	if i >= l.LastIndex() {
		return
	}
	// Before return, need update the stable
	defer func() {
		l.stabled = min(l.stabled, i)
	}()
	i = max(l.hadCompacted(), i)
	// delete the entries which are not been compacted and greater than i
	offset := l.entries[0].Index
	// `i` may less than offset, and it's a uint64, add brackets to avoid underflow
	l.entries = l.entries[0 : (i+1)-offset]
}

func (l *RaftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i {
		log.Panicf("Try to applied to %d, but the commited is also %d", i, l.committed)
	}
	if i < l.applied {
		log.Panicf("Try to applied to %d, but the old applied is %d", i, l.committed)
	}
	l.applied = i
}

// hadCompacted will return the highest position of log which is compacted.
func (l *RaftLog) hadCompacted() uint64 {
	snapshot, ssErr := l.storage.Snapshot()
	if ssErr != nil {
		panic(ssErr)
	}
	return snapshot.Metadata.Index
}

func (l *RaftLog) stableTo(i uint64) {
	li := l.LastIndex()
	if i > li {
		log.Panicf("Try stabled to %d, but lastIndex is %d", i, li)
	}
	if i < l.stabled {
		log.Panicf("Try stabled to %d, but oldStabled is %d", i, l.stabled)
	}
	l.stabled = i
}
