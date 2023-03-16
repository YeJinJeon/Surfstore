package surfstore

import (
	context "context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	//additional
	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: s.metaStore.FileMetaMap}, nil
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	c := s.metaStore.ConsistentHashRing
	bsm := make(map[string][]string)
	for _, blockHash := range hashes.Hashes {
		serverName := c.GetResponsibleServer(blockHash)
		bsm[serverName] = append(bsm[serverName], blockHash)
	}
	blockStoreMap := make(map[string]*BlockHashes)
	for server, blockhashes := range bsm {
		blockStoreMap[server] = &BlockHashes{Hashes: blockhashes}
	}
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: s.metaStore.BlockStoreAddrs}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	fmt.Printf("============= UpdateFile: %d =============\n", s.id)
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	} else {
		fmt.Printf("[Leader %d]: {Term: %d}, {Log: %s}, {Commited: %d}, {Applied: %d}\n", s.id, s.term, s.log, s.commitIndex, s.lastApplied)
	}
	//append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	//send entry to all followers
	go s.sendToAllFollowersInParallel(ctx)

	//keep trying indefinitely (even after responding) ** rely on sendHearbeat

	//commit the entry once majority of followers have it in their log
	commit := <-commitChan

	//once commited, apply to the state machine
	if commit {
		fmt.Println("**********SUCCESS COMMITTED*********")
		s.lastApplied++
		fmt.Printf("[Leader %d]: {Term: %d}, {Log: %s}, {Commited: %d}, {Applied: %d}\n", s.id, s.term, s.log, s.commitIndex, s.lastApplied)
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	//send entry to all my followers and count replies
	responses := make(chan bool, len(s.peers)-1)
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		go s.sendToFollower(ctx, addr, responses)
	}

	totalResponses := 1
	totalAppends := 1

	//wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}
	if totalAppends > len(s.peers)/2 {
		// TODO: put on correct channel
		*s.pendingCommits[len(s.pendingCommits)-1] <- true
		// TODO: update commit index with right value
		s.commitIndex++
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, responses chan bool) {
	var prev_logIndex int64
	var prev_logTerm int64
	if len(s.log) > 0 {
		prev_logIndex = int64(len(s.log) - 1)
		prev_logTerm = s.log[len(s.log)-1].Term
	} else {
		prev_logIndex = int64(-1)
		prev_logTerm = int64(0)
	}
	dummyAppendEntriesInput := AppendEntryInput{
		// TODO: put the right values
		Term:         s.term,
		PrevLogIndex: prev_logIndex,
		PrevLogTerm:  prev_logTerm,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}
	// TODO: check all errors
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)

	_, _ = client.AppendEntries(ctx, &dummyAppendEntriesInput)

	// TODO: check output
	responses <- true
}

// 1. Reply false if term < currentTerm (§5.1) --> term is out-dated
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)

func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	/*
		input: Entry Input from the node claiming to be a leader
		s: follower(client)
	*/

	if input.Term < s.term {
		fmt.Println("-------------> [setLeader] ERR NOT LEADER")
		return &AppendEntryOutput{Term: s.term}, ERR_NOT_LEADER
	}

	if input.Term > s.term { // [setLeader] sendHeartbeat: if follower term is smaller
		fmt.Println("-------------> [setLeader] SendHeartbeat")
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
		fmt.Printf("[Client %d]: {Term: %d}, {Log: %s}, {Commited: %d}, {Applied: %d}\n", s.id, s.term, s.log, s.commitIndex, s.lastApplied)
	}

	// // TODO actually check entries
	// if len(s.log) == 0 || len(s.log) < Input.PrevLogIndex-1 { //refuses to append new entreis
	// 	return false, nil
	// } else if {

	// }

	// for s.lastApplied < input.LeaderCommit {
	// 	entry := s.log[s.lastApplied+1]
	// 	s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	// 	s.lastApplied++
	// }
	// else if s.lastApplied == input.LeaderCommit { // update
	// 	fmt.Println("-------------> UpdateFile (Update Client Log)")
	// 	fmt.Printf("[Client %d]: {Term: %d}, {Log: %s}, {Commited: %d}, {Applied: %d}\n", s.id, s.term, s.log, s.commitIndex, s.lastApplied)
	// 	// if the follower does not find an Prev Entry in its log with the same index and term
	// 	// refuses the new entries
	// 	if len(s.log) == 0 && len(input.Entries) > 0 { //inital entry append of follower
	// 		s.log = input.Entries
	// 	} else if s.log[input.PrevLogIndex].Term == input.PrevLogTerm && len(input.Entries) > 0 { // follower log is not empty
	// 		s.log = input.Entries
	// 	} else {
	// 		fmt.Println("how to refuse the new entries?")
	// 	}
	// 	fmt.Printf("[Client %d]: {Term: %d}, {Log: %s}, {Commited: %d}, {Applied: %d}\n", s.id, s.term, s.log, s.commitIndex, s.lastApplied)
	// } else { // [UpdateFile] sendHeartbeat:  apply to metastore(state matchine) of followers
	// 	fmt.Println("-------------> [UpdateFile] SendHeartbeat")
	// 	// s.lastApplied < input.LeaderCommit
	// 	s.commitIndex++
	// 	entry := s.log[s.lastApplied+1]
	// 	s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	// 	s.lastApplied++
	// 	fmt.Printf("[Client %d]: {Term: %d}, {Log: %s}, {Commited: %d}, {Applied: %d}\n", s.id, s.term, s.log, s.commitIndex, s.lastApplied)
	// }
	return nil, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Printf("============= Set Leader: %d =============\n", s.id)
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++
	fmt.Printf("[Client %d]: {Term: %d}, {Log: %s}, {Commited: %d}, {Applied: %d}\n", s.id, s.term, s.log, s.commitIndex, s.lastApplied)

	//TODO: update state

	return nil, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Printf("============= SendHearbeat: %d =============\n", s.id)

	// sendHearbeat by followers
	if !s.isLeader {
		return &Success{Flag: false}, nil
	}

	var prev_logIndex int64
	var prev_logTerm int64
	if len(s.log) > 0 {
		prev_logIndex = int64(len(s.log) - 1)
		prev_logTerm = s.log[len(s.log)-1].Term
	} else {
		prev_logIndex = int64(-1)
		prev_logTerm = int64(0)
	}
	//contact all the followers, and send some AppendEntries call
	dummyAppendEntriesInput := AppendEntryInput{
		//TODO: put the right values
		Term:         s.term,
		PrevLogIndex: prev_logIndex,
		PrevLogTerm:  prev_logTerm,
		Entries:      make([]*UpdateOperation, 0),
		LeaderCommit: s.commitIndex,
	}
	fmt.Printf("[Input Entry]: {Term: %d}, {Entries:%s}, {Commit: %d}\n", dummyAppendEntriesInput.Term, dummyAppendEntriesInput.Entries, dummyAppendEntriesInput.LeaderCommit)

	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		conn, _ := grpc.Dial(addr, grpc.WithInsecure())
		client := NewRaftSurfstoreClient(conn)

		_, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		if errors.Is(err, ERR_NOT_LEADER) {
			s.isLeaderMutex.Lock()
			defer s.isLeaderMutex.Unlock()
			s.isLeader = false
			return &Success{Flag: false}, nil
		}
	}
	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Printf(" !!!!!!!!!!!!!!! Crashed: %d\n", s.id)
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Printf(" ^^^^^^^^^^^^^^^ Restored: %d\n", s.id)
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
