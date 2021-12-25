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
	"fmt"
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
	// the interval of maintaining current state without receiving messages
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// baseline of election interval
	electionTimeoutBase int

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
	raft := new (Raft)
	hardState, _, _ := c.Storage.InitialState()
	raft.id = c.ID
	raft.Term = hardState.Term
	raft.Vote = hardState.Vote
	raft.RaftLog = newLog(c.Storage)
	// Init Match index as 0 and Next index as 1 for each nodes
	raft.Prs = make(map[uint64]*Progress)
	for _, id := range c.peers {
		raft.Prs[id] = &Progress{
			Match: 0,
			Next: 1,
		}
	}
	raft.State = StateFollower
	// votes, msgs, Lead, leadTransferee and PendingConfIndex are not initialized
	// Randomize election timeout to avoid split vote
	raft.heartbeatTimeout = c.HeartbeatTick
	raft.electionTimeoutBase = c.ElectionTick
	raft.setRandomElectionTimeout()
	raft.heartbeatElapsed = 0
	raft.electionElapsed = 0
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}

	var msg pb.Message
	msg.MsgType = pb.MessageType_MsgAppend
	msg.To = to
	msg.From = r.id
	msg.Term = r.Term
	progress := r.Prs[to]
	if progress == nil {
		return false
	}
	// Set prevLogIndex and prevLogTerm to msg
	msg.Index = progress.Next - 1
	var err error
	msg.LogTerm, err = r.RaftLog.Term(msg.Index - 1)
	if err != nil {
		return false
	}
	// Copy entries and set leaderCommit to msg
	msg.Entries = transferListToPointerList(r.RaftLog.deepCopyEntriesAfter(progress.Next))
	if msg.Entries == nil {
		return false
	}
	msg.Commit = r.RaftLog.committed

	r.sendMessage(msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// Return true if a heartbeat is sent
func (r *Raft) sendHeartbeat(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}
	var msg pb.Message
	msg.MsgType = pb.MessageType_MsgHeartbeat
	msg.To = to
	msg.From = r.id
	msg.Term = r.Term

	r.sendMessage(msg)
	return true
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		// For a leader, it just needs to send heartbeat periodically
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			for id, _ := range r.Prs {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		}
	} else {
		// For a candidate or follower, if it hasn't got the message from
		// the leader for a long time, it will become a candidate and
		// start an election
		r.electionElapsed++
		if r.electionElapsed == r.electionTimeout {
			r.becomeCandidate()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) error {
	// Your Code Here (2A).
	// Do sanity check: whether term is outdated or potential leader is itself
	if term < r.Term {
		return fmt.Errorf("Fail to become a follower of an outdated leader")
	}
	if lead == r.Lead {
		return fmt.Errorf("Fail to become a follower of itself")
	}
	// Synchronize to leader's term and return no error
	r.Term = term
	r.Vote = lead
	r.State = StateFollower
	r.Lead = lead
	// Reset election timeout
	r.electionElapsed = 0
	err := r.setRandomElectionTimeout()
	if err != nil {
		return err
	}
	return nil
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() error {
	// Your Code Here (2A).
	// Start election in a new term and reset election timer
	r.Term += 1
	// Vote for itself
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.State = StateCandidate
	// Reset election timeout
	r.electionElapsed = 0
	r.setRandomElectionTimeout()
	// Send RequestVote RPC to all the other servers ???
	return nil
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	// Reset election timeout
	r.electionElapsed = 0
	r.setRandomElectionTimeout()
	r.heartbeatElapsed = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// The leader doesn't need to handle her applyEntry request
	if r.id == m.From {
		return
	}

	var response pb.Message
	response.MsgType = pb.MessageType_MsgAppendResponse
	response.To = m.From
	response.From = r.id
	response.Term = r.Term
	// 1. Reject the outdated applyEntries
	if m.Term < r.Term {
		response.Reject = true
		r.sendMessage(response)
		return
	}
	// 2. Reject the applyEntries with an unmatched entry
	// at prevLogIndex in prevLogTerm
	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm
	actualPrevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	if prevLogTerm != actualPrevLogTerm {
		response.Reject = true
		r.sendMessage(response)
		return
	}
	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it
	existingEntries := r.RaftLog.deepCopyEntriesAfter(m.Index + 1)
	// delStartIndex is the first index where the existing entry has conflict
	// with the corresponding entry in message
	var delStartIndex uint64
	minLen := min(uint64(len(existingEntries)), uint64(len(m.Entries)))
	for delStartIndex = 0; delStartIndex < minLen; delStartIndex++ {
		if !r.RaftLog.entries[delStartIndex].IsEqualEntry(*(m.Entries[delStartIndex])) {
			break
		}
	}
	// If delStartIndex >= len(existingEntries), this operation deletes nothing
	r.RaftLog.deleteEntriesAfter(m.Index + 1 + delStartIndex)
	// 4. Append any new entries not already in the log
	if delStartIndex < uint64(len(m.Entries)) {
		r.RaftLog.entries = append(r.RaftLog.entries,
				transferPointerListToList(m.Entries[delStartIndex:]) ...)
	}
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if m.Commit > r.RaftLog.committed {
		lastNewEntryIndex := r.RaftLog.LastIndex()
		r.RaftLog.committed = min(m.Commit, lastNewEntryIndex)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// The leader doesn't need to handle her heartbeat
	if r.id == m.From {
		return
	}
	var response pb.Message
	response.MsgType = pb.MessageType_MsgHeartbeatResponse
	response.To = m.From
	response.From = r.id
	response.Term = r.Term
	// Reject the outdated heartbeat
	if m.Term < r.Term {
		response.Reject = true
	} else {
		response.Reject = false
	}
	r.sendMessage(response)
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

// sendMessage sends the message msg
// by appending it into message list msgs
func (r *Raft) sendMessage(msg pb.Message) {
	r.msgs = append(r.msgs, msg)
}

// setRandomElectionTimeout sets electionTimeout
// as rand[electionTimeoutBase, 1.5*electionTimeoutBase] to avoid split vote
// If electionTimeoutBase <= 0, return an error
func (r *Raft) setRandomElectionTimeout() error {
	if r.electionTimeoutBase <= 0 {
		return fmt.Errorf("electionTimeoutBase should be positive")
	}

	rand.Seed(time.Now().Unix())
	r.electionTimeout = r.electionTimeoutBase + rand.Intn(r.electionTimeoutBase) / 2
	return nil
}
