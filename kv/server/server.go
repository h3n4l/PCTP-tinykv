package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
	"log"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{
		RegionError: nil,
		Value:       nil,
		NotFound:    true,
	}
	reader, _ := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, req.Version)

	lock, _ := txn.GetLock(req.Key)
	if lock != nil && lock.Ts < req.Version {
		li := &kvrpcpb.LockInfo{
			PrimaryLock: lock.Primary,
			LockVersion: lock.Ts,
			Key:         req.Key,
			LockTtl:     lock.Ttl,
		}
		resp.Error = &kvrpcpb.KeyError{Locked: li}
		return resp, nil
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
	}
	if value != nil {
		resp.Value = value
		resp.NotFound = false
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	reader, _ := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	i := len(req.Mutations)
	log.Println("mutation len : ", i)
	for _, m := range req.Mutations {
		write, cTs, _ := txn.MostRecentWrite(m.Key)
		if write != nil {
			if cTs >= req.StartVersion {
				wc := &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: cTs,
					Key:        m.Key,
					Primary:    req.PrimaryLock,
				}
				resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Conflict: wc})
				continue
			}
		}
		lock, _ := txn.GetLock(m.Key)
		if lock != nil && lock.Ts < req.StartVersion {
			li := &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         m.Key,
				LockTtl:     lock.Ttl,
			}
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{Locked: li})
			continue
		}
		var wk mvcc.WriteKind
		if m.Op == kvrpcpb.Op_Put {
			wk = mvcc.WriteKindPut
			txn.PutValue(m.Key, m.Value)
		}
		if m.Op == kvrpcpb.Op_Del {
			wk = mvcc.WriteKindDelete
			txn.DeleteValue(m.Key)
		}
		l := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    wk,
		}
		txn.PutLock(m.Key, l)
		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
		}
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	reader, _ := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, req.CommitVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	for _, k := range req.Keys {
		lock, _ := txn.GetLock(k)
		if lock == nil {
			return resp, nil
		}
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{Retryable: "retry"}
			return resp, nil
		}
		txn.DeleteLock(k)
		w := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindPut,
		}
		txn.PutWrite(k, req.CommitVersion, w)
		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
		}
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, _ := server.storage.Reader(req.Context)
	//???? req.currentTs
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	key := req.PrimaryKey
	write, cTs, _ := txn.CurrentWrite(key)
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = cTs
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		}
		if write.Kind == mvcc.WriteKindRollback {
			resp.Action = kvrpcpb.Action_NoAction
			return resp, nil
		}
	}
	lock, _ := txn.GetLock(key)
	if lock == nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		w := &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(key, req.LockTs, w)
		err := server.storage.Write(req.Context, txn.Writes())
		log.Println(err)
		return resp, nil
	}
	if lock != nil {
		if mvcc.PhysicalTime(req.CurrentTs) > mvcc.PhysicalTime(lock.Ts)+lock.Ttl {
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			txn.DeleteValue(key)
			txn.DeleteLock(key)
			w := &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			}
			txn.PutWrite(key, req.LockTs, w)
			err := server.storage.Write(req.Context, txn.Writes())
			log.Println(err)
			return resp, nil
		}
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}
	resp.Action = kvrpcpb.Action_NoAction
	resp.CommitVersion = resp.LockTtl
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, _ := server.storage.Reader(req.Context)
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	log.Println("Keys: ", req.Keys)
	for _, k := range req.Keys {
		write, _, _ := txn.CurrentWrite(k)
		if write != nil && write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{Abort: "txn aborted"}
		}
		if write.Kind == mvcc.WriteKindRollback {
			return resp, nil
		}
		w := &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(k, req.StartVersion, w)
		txn.DeleteLock(k)
		txn.DeleteValue(k)
	}
	err := server.storage.Write(req.Context, txn.Writes())
	log.Println(err)
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
