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
)

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

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	return nil
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
		return []pb.Entry{}
	}
	if l.stabled == l.LastIndex() {
		return []pb.Entry{}
	}
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
		return []pb.Entry{}
	}
	if l.committed == l.applied {
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
		//先进性持久化再进行压缩 当日志被压缩的时候 日志为空时候 stabled记录的就是压缩的最后一条日志
		return l.stabled
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	if l.isEntriesEmpty() {
		return 0, EntriesUnavailable
	}
	offset := l.entries[0].Index
	if i > l.LastIndex() || i < offset {
		return 0, EntriesUnavailable
	}
	return l.entries[i-offset].Term, nil
}

func (l *RaftLog) isEntriesEmpty() bool {
	return len(l.entries) == 0
}

func (l *RaftLog) append(ents ...pb.Entry) {
	if len(ents) == 0 {
		return
	}
	l.entries = append(l.entries, ents...)
}

func (l *RaftLog) tryCommit(i uint64) bool {
	if i <= l.committed {
		return false
	}
	l.committed = min(i, l.LastIndex())
	return true
}
