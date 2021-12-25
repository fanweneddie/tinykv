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
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

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
	log := new(RaftLog)
	log.storage = storage
	// Entries prior to first entries are committed and applied
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic("error in get first index: " + err.Error())
	}
	// Entries prior to lastIndex is stabilized
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic("error in get last index: " + err.Error())
	}
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	log.stabled = lastIndex

	// Copy the entries from storage
	if firstIndex <= lastIndex {
		storageEntries, err := storage.Entries(firstIndex, lastIndex + 1)
		if err != nil {
			panic("error in copying storage entries: " + err.Error())
		}
		log.entries = make([]pb.Entry, len(storageEntries))
		copy(log.entries, storageEntries)
	}

	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
// If entries does not contain an unstable entry, return an empty list
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l == nil || l.entries == nil {
		return nil
	}
	if len(l.entries) == 0 {
		return make([]pb.Entry, 0)
	}
	// Find the entry whose index > stabled
	startIndex, found := l.getEntryPositionByIndex(l.stabled + 1)
	if !found {
		return make([]pb.Entry, 0)
	} else {
		unstableEntries := l.deepCopyEntriesAfter(startIndex)
		return unstableEntries
	}
}

// nextEnts returns all the committed but not applied entries
// If committed index < applied index or any one of them does not
// have a corresponding entry in entries, then return an empty list
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if l == nil || l.entries == nil {
		return nil
	}

	if l.committed < l.applied {
		return make([]pb.Entry, 0)
	}

	committedIndex, committedFound := l.getEntryPositionByIndex(l.committed)
	appliedIndex, appliedFound := l.getEntryPositionByIndex(l.applied)
	if !committedFound || !appliedFound {
		return make([]pb.Entry, 0)
	} else {
		return l.deepCopyEntriesBetween(appliedIndex + 1, committedIndex)
	}
}

// LastIndex return the last index of the log entries
// If entries does not contain any entry, return 0
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if l == nil || l.entries == nil {
		return 0
	}

	entriesLen := len(l.entries)
	if entriesLen == 0 {
		return 0
	} else {
		return l.entries[entriesLen-1].Index
	}
}

// Term return the term of the entry in the given index
// If the entry with index i hasn't been found, return an error
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if l == nil || l.entries == nil {
		return 0, fmt.Errorf("RaftLog or its entries is nil")
	}

	pos, found := l.getEntryPositionByIndex(i)
	if !found {
		return 0, fmt.Errorf("fail to find the entry with index %d in entries", i)
	} else {
		return l.entries[pos].Term, nil
	}
}

// deleteEntriesAfter deletes entries whose index >= start
// If start >= len(l.entries), it does nothing
func (l *RaftLog) deleteEntriesAfter(start uint64) {
	if l == nil || l.entries == nil {
		return
	}

	len := uint64(len(l.entries))
	if start >= len {
		return
	}
	// Assign entries with its original subarray
	if start == 0 {
		l.entries = make([]pb.Entry, 0)
	} else {
		l.entries = l.entries[0:start]
	}
}

// deepCopyEntriesAfter deep copies l.entries[start:] into dst
// If start >= len(l.entries), return an empty list
func (l *RaftLog) deepCopyEntriesAfter(start uint64) []pb.Entry {
	if l == nil || l.entries == nil {
		return nil
	}

	len := uint64(len(l.entries))
	if start >= len {
		return make([]pb.Entry, 0)
	}

	dst := make([]pb.Entry, len - start)
	copy(dst, l.entries[start:])
	return dst
}

// deepCopyEntriesBetween deep copies l.entries[start,end] into dst
// If start > end, return an empty list
// if [start, end] is not included by [0, len(entries)-1], return an empty list
func (l *RaftLog) deepCopyEntriesBetween(start uint64, end uint64) []pb.Entry {
	if l == nil || l.entries == nil {
		return nil
	}

	if start > end {
		return make([]pb.Entry, 0)
	}

	if !(start >= 0 && end <= uint64(len(l.entries)) - 1) {
		return make([]pb.Entry, 0)
	}

	dst := make([]pb.Entry, end - start + 1)
	copy(dst, l.entries[start: end + 1])
	return dst
}

// getEntryPositionByIndex gets the position of entry in entries
// which contains the given index
// If the entry exists, return the index and true
// Else, return 0 and false
func (l *RaftLog) getEntryPositionByIndex(index uint64) (uint64, bool) {
	if l == nil || l.entries == nil || len(l.entries) == 0 {
		return 0, false
	}

	startIndex := l.entries[0].Index
	// Since index in entries is increasing,
	// entries does not contain an entry with that index
	if startIndex > index {
		return 0, false
	}

	offset := index - startIndex
	// Entries is too short to contain the index
	if offset >= uint64(len(l.entries)) {
		return 0, false
	}

	return offset, true
}

// transferListToPointerList transfers a list of entries to a list of entry pointers
// If entries is nil, then return nil
func transferListToPointerList(entries []pb.Entry) []*pb.Entry {
	if entries == nil {
		return nil
	}

	dst := make([]*pb.Entry, len(entries))
	for i, entry := range entries {
		entryPtr := new(pb.Entry)
		*entryPtr = entry
		dst[i] = entryPtr
	}
	return dst
}

// transferPointerListToList transfers a list of entry pointers to list of entries
// If entries is nil, then return nil
func transferPointerListToList(entryPtrs []*pb.Entry) []pb.Entry {
	if entryPtrs == nil {
		return nil
	}

	dst := make([]pb.Entry, len(entryPtrs))
	for i, entryPtr := range entryPtrs {
		dst[i] = *entryPtr
	}
	return dst
}
