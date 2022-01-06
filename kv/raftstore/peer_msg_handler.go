package raftstore

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()
	//TODO: handle the snapshot
	applySnapRes, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		log.Panic(err)
	}
	if applySnapRes != nil {
		if !reflect.DeepEqual(applySnapRes.PrevRegion, applySnapRes.Region) {
			// Update RegionLocalState
			d.peerStorage.SetRegion(applySnapRes.Region)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[applySnapRes.Region.Id] = applySnapRes.Region
			storeMeta.regionRanges.Delete(&regionItem{region: applySnapRes.PrevRegion})
			storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: applySnapRes.Region})
			storeMeta.Unlock()
		}
	}
	//Send the msg
	for _, msg := range rd.Messages {
		d.sendRaftMessage(msg, d.ctx.trans)
	}
	// handle apply
	if len(rd.CommittedEntries) > 0 {
		//log.Infof("[Region: %d] Peer %d **Begin** Apply from %d to %d", d.regionId, d.PeerId(), d.peerStorage.applyState.AppliedIndex, d.peerStorage.applyState.AppliedIndex+uint64(len(rd.CommittedEntries)))

		for _, ent := range rd.CommittedEntries {
			switch ent.EntryType {
			case eraftpb.EntryType_EntryNormal:
				//d.processNormalCommitted(ent)
				d.processNormalCommittedEntry(ent)
			case eraftpb.EntryType_EntryConfChange:
				// TODO: handle the conf change entry
				d.processConfChangeEntry(ent)
				if d.stopped {
					// This peer destroys itself
					return
				}
			}
		}
		//log.Infof("[Region: %d] Peer %d Apply from %d to %d", d.regionId, d.PeerId(), d.peerStorage.applyState.AppliedIndex-uint64(len(rd.CommittedEntries)), d.peerStorage.applyState.AppliedIndex)
	}
	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) processConfChangeEntry(ent eraftpb.Entry) {
	// Unmarshall to get the reqeust
	kvWB := new(engine_util.WriteBatch)
	var cc eraftpb.ConfChange
	err := proto.Unmarshal(ent.Data, &cc)
	if err != nil {
		panic(err)
	}
	var cmdReq raft_cmdpb.RaftCmdRequest
	err = proto.Unmarshal(cc.Context, &cmdReq)
	if err != nil {
		panic(err)
	}
	prop := d.handleProposal(ent.Index, ent.Term)
	key := d.getKey(&cmdReq)
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			if prop != nil {
				prop.cb.Done(ErrResp(err))
			}
			kvWB := new(engine_util.WriteBatch)
			d.peerStorage.applyState.AppliedIndex += 1
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
			return
		}
	}
	err = util.CheckRegionEpoch(&cmdReq, d.Region(), true)
	if err != nil {
		if prop != nil {
			prop.cb.Done(ErrResp(err))
		}
		kvWB := new(engine_util.WriteBatch)
		d.peerStorage.applyState.AppliedIndex += 1
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		return
	}
	err = util.CheckRegionEpoch(&cmdReq, d.Region(), true)

	if epochNotMatch, ok := err.(*util.ErrEpochNotMatch); ok {
		if prop != nil {
			prop.cb.Done(ErrResp(epochNotMatch))
		}
		d.peerStorage.applyState.AppliedIndex += 1
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		return
	}
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		if d.getPeerFromCache(cc.NodeId) == nil {
			region := d.Region()
			peer := cmdReq.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers, peer)
			log.Infof("[Region:%d] Peer %d add RegionEpoch.ConfVer from %d to %d because of add node %d", d.regionId, d.PeerId(), region.RegionEpoch.ConfVer, region.RegionEpoch.ConfVer+1, peer.Id)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			sm := d.ctx.storeMeta
			sm.Lock()
			sm.regions[region.Id] = region
			sm.Unlock()
			d.insertPeerCache(peer)
		}
	case eraftpb.ConfChangeType_RemoveNode:
		if cc.NodeId == d.Meta.Id {
			// remove myself
			d.destroyPeer()
			return
		}
		if d.getPeerFromCache(cc.NodeId) != nil {
			region := d.Region()
			peer := cmdReq.AdminRequest.ChangePeer.Peer
			pos := -1
			for idx, p := range d.Region().Peers {
				if p.Id == peer.Id {
					pos = idx
					break
				}
			}
			region.Peers = append(region.Peers[:pos], region.Peers[pos+1:]...)
			log.Infof("[Region:%d] Peer %d add RegionEpoch.ConfVer from %d to %d because of remove node %d", d.regionId, d.PeerId(), region.RegionEpoch.ConfVer, region.RegionEpoch.ConfVer+1, peer.Id)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			sm := d.ctx.storeMeta
			sm.Lock()
			sm.regions[region.Id] = region
			sm.Unlock()
			d.removePeerCache(peer.Id)
		}
	}
	d.RaftGroup.ApplyConfChange(cc)
	d.peerStorage.applyState.AppliedIndex += 1
	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	if prop != nil {
		prop.cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{
				Error:       nil,
				Uuid:        nil,
				CurrentTerm: d.Term(),
			},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: cmdReq.AdminRequest.CmdType,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{
					Region: d.Region(),
				},
			},
		})
	}
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

// processNormalCommittedEntry will handle the committed entry, included response, apply...
func (d *peerMsgHandler) processNormalCommittedEntry(ent eraftpb.Entry) {
	// For Noop entry
	if ent.Data == nil {
		prop := d.handleProposal(ent.Index, ent.Term)
		// To update the applied state to kv db
		kvWB := new(engine_util.WriteBatch)
		d.peerStorage.applyState.AppliedIndex += 1
		kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		if prop!=nil{
			prop.cb.Done(ErrRespStaleCommand(d.Term()))
		}
		return
	}
	// Unmarshall to get the request
	var cmdRequest raft_cmdpb.RaftCmdRequest
	err := proto.Unmarshal(ent.Data, &cmdRequest)
	if err != nil {
		panic(err)
	}
	prop := d.handleProposal(ent.Index, ent.Term)
	//if key != nil {
	//	err := util.CheckKeyInRegion(key, d.Region())
	//	if err != nil {
	//		if prop != nil {
	//			prop.cb.Done(ErrResp(err))
	//		}
	//		log.Infof("[Region: %d] Peer: %d refuse to applied entry [%d,%d], because of key not match, region:[%s:%s], req: %s", d.regionId, d.PeerId(), ent.Index, ent.Term, d.Region().StartKey, d.Region().EndKey, key)
	//		kvWB := new(engine_util.WriteBatch)
	//		d.peerStorage.applyState.AppliedIndex += 1
	//		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	//		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	//		return
	//	}
	//}
	//err = util.CheckRegionEpoch(&cmdRequest, d.Region(), true)
	//if err != nil {
	//	if prop != nil {
	//		prop.cb.Done(ErrResp(err))
	//	}
	//	log.Infof("[Region: %d] Peer: %d refuse to applied entry [%d,%d], because of region epoch not match, peer:%v, req: %v", d.regionId, d.PeerId(), ent.Index, ent.Term, d.Region().RegionEpoch, cmdRequest.Header.RegionEpoch)
	//	kvWB := new(engine_util.WriteBatch)
	//	d.peerStorage.applyState.AppliedIndex += 1
	//	kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	//	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	//	return
	//}
	if cmdRequest.AdminRequest != nil {
		// TODO: handle admin request
		d.appliedAdminRequest(&cmdRequest, prop)
	} else if len(cmdRequest.Requests) != 0 {
		hadresped := d.appliedNormalRaftCmdRequest(cmdRequest.Requests,prop)
		if prop != nil && !hadresped {
			resp := d.getResponseOfNormalRaftCmdRequest(cmdRequest.Requests, prop,cmdRequest.Header.RegionEpoch)
			prop.cb.Done(resp)
		}
	}
}

func (d *peerMsgHandler) appliedAdminRequest(req *raft_cmdpb.RaftCmdRequest, prop *proposal) {
	resp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{
			Error:       nil,
			Uuid:        nil,
			CurrentTerm: d.Term(),
		},
	}
	switch req.AdminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactReq := req.AdminRequest.CompactLog
		if compactReq.CompactIndex >= d.peerStorage.truncatedIndex() {
			// notify to truncated
			d.peerStorage.applyState.TruncatedState.Index = compactReq.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = compactReq.CompactTerm
			// schedule a task to raftlog-gc worker
			d.ScheduleCompactLog(compactReq.CompactIndex)
			kvWB := new(engine_util.WriteBatch)
			d.peerStorage.applyState.AppliedIndex += 1
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		} else {
			d.ScheduleCompactLog(d.peerStorage.truncatedIndex())
		}
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
			CompactLog: &raft_cmdpb.CompactLogResponse{},
		}
		if prop != nil {
			prop.cb.Done(resp)
		}
	case raft_cmdpb.AdminCmdType_Split:
		splitReq := req.AdminRequest.Split
		region := d.Region()
		// check region epoch
		err := util.CheckRegionEpoch(req, region, true)
		if errEpochNotMatch, ok := err.(*util.ErrEpochNotMatch); ok {
			if prop != nil {
				prop.cb.Done(ErrResp(errEpochNotMatch))
			}
			return
		}
		// check key
		err = util.CheckKeyInRegion(splitReq.SplitKey, region)
		if err != nil {
			if prop != nil {
				prop.cb.Done(ErrResp(err))
			}
			return
		}
		sm := d.ctx.storeMeta
		sm.Lock()
		// replace old region, prepare replace
		// sm.regionRanges.Delete(&regionItem{region: region})
		region.RegionEpoch.Version++
		// prepare new peers in new region
		peers := make([]*metapb.Peer, 0)
		for i, peer := range region.Peers {
			peers = append(peers, &metapb.Peer{
				Id:      splitReq.NewPeerIds[i],
				StoreId: peer.StoreId,
			})
		}
		newRegion := &metapb.Region{
			Id:       splitReq.NewRegionId,
			StartKey: splitReq.SplitKey,
			EndKey:   region.EndKey,
			// new region epoch
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: peers,
		}
		log.Infof("[Region: %d] [%s:%s] Peer %d split to 2 regions: \n\t[Region: %d] [%s:%s] \t [Region: %d] [%s:%s]", region.Id, region.StartKey, region.EndKey, d.PeerId(), region.Id, region.StartKey, splitReq.SplitKey, newRegion.Id, newRegion.StartKey, newRegion.EndKey)
		sm.regions[newRegion.Id] = newRegion
		region.EndKey = splitReq.SplitKey
		// Insert in storemeta
		sm.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		sm.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		sm.Unlock()
		kvWB := new(engine_util.WriteBatch)
		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
		// update applied state
		d.peerStorage.applyState.AppliedIndex += 1
		kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
		kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)
		peer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		// Register in router
		d.ctx.router.register(peer)
		d.ctx.router.send(newRegion.Id, message.Msg{
			Type:     message.MsgTypeStart,
		})
		if prop != nil {
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_Split,
				Split: &raft_cmdpb.SplitResponse{
					Regions: []*metapb.Region{region, newRegion},
				},
			}
			prop.cb.Done(resp)
		}
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
	}
}

// appliedNormalRaftCmdRequest applied the request in `reqs`
func (d *peerMsgHandler) appliedNormalRaftCmdRequest(reqs []*raft_cmdpb.Request,prop *proposal) (hadResponed bool) {
	if len(reqs) != 1 {
		log.Panicf("many reqs")
	}
	for i := 1; i < len(reqs); i++ {
		if reqs[i].CmdType != reqs[i-1].CmdType {
			log.Panicf("Many normal request type")
		}
	}
	// New a writeBranch
	kvWB := new(engine_util.WriteBatch)
	req := reqs[0]
	key := getRequestKey(req)
	if key != nil{
		keyErr := util.CheckKeyInRegion(key,d.Region())
		if keyErr != nil{
			if prop != nil{
				d.peerStorage.applyState.AppliedIndex += 1
				kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
				kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
				prop.cb.Done(ErrResp(keyErr))
				return true
			}
			return true
		}
	}
	// Apply to kv db
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
	case raft_cmdpb.CmdType_Put:
		kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
	case raft_cmdpb.CmdType_Delete:
		kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
	case raft_cmdpb.CmdType_Snap:
		
	}
	// Update the applied index
	d.peerStorage.applyState.AppliedIndex += 1
	kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState)
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	return false
}

// getResponseOfNormalRaftCmdRequest will return the response of request and proposal, caller need protect
// reqs are not nil and prop is not nil
func (d *peerMsgHandler) getResponseOfNormalRaftCmdRequest(reqs []*raft_cmdpb.Request, prop *proposal,re *metapb.RegionEpoch) *raft_cmdpb.RaftCmdResponse {
	if len(reqs) != 1 {
		log.Panicf("many reqs")
	}
	for i := 1; i < len(reqs); i++ {
		if reqs[i].CmdType != reqs[i-1].CmdType {
			log.Panicf("Many normal request type")
		}
	}
	req := reqs[0]
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: &raft_cmdpb.RaftResponseHeader{
			CurrentTerm: d.Term(),
		},
	}
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
		if err != nil {
			value = nil
		}
		cmdResp.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Get,
			Get: &raft_cmdpb.GetResponse{
				Value: value,
			},
		}}
	case raft_cmdpb.CmdType_Put:
		cmdResp.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Put,
		}}
	case raft_cmdpb.CmdType_Delete:
		cmdResp.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Delete,
		}}
	case raft_cmdpb.CmdType_Snap:
		if re.Version != d.Region().RegionEpoch.Version{
			return ErrResp(&util.ErrEpochNotMatch{})
		}
		cmdResp.Responses = []*raft_cmdpb.Response{{
			CmdType: raft_cmdpb.CmdType_Snap,
			Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
		}}
		prop.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
	}
	return cmdResp

}

// handleProposal try to return the proposal in peerMsgHandler which index and term are matched with args, and
// it will notify stale command of that prop.index < index || (prop.index == index && prop.term != term), if no
// match proposal, it will return nil after notify stale proposal.
func (d *peerMsgHandler) handleProposal(index, term uint64) *proposal {
	if len(d.proposals) == 0 {
		return nil
	}
	// Match the index and the term
	// There are some situations that we need consider:
	// Assume we have 3 nodes(1,2,3)
	// A: 1 is the leader in term1, and receive propose from clients, save it as log[index:1,term1].
	// 1 crashed before commit it, and 2 is the leader in term 2, receive propose from clients, save it as
	// log[index:1, term:2], and commit and apply it. 1 will overwrite its log to consist with 2. And it
	// apply log[index:1,term:2] will discover that it has a proposal[index:1, term:1], you will find that
	// index is equal, but term are different, it needs return StaleCommand.

	// B: As same as situation A, but 1 failed after it commit but before applying, 2 is the leader in term2,
	// receive propose from clients, save it as log[index:2,term:2], and commit and apply. When 1 restarts,
	// it will apply the log[index:1,term:1] and handle the proposal[index:1,term:1]

	// C: As same as situation B, but 2 failed before apply, and 3 become leader, receive a proposal, save it
	// as log[index:3,term 3], and copy it to 1. And failed before apply again, and 1 become leader, receive
	// propose from clients, save it as log[index:4, term:4]. Now, 1 has 2 proposals[index:1,term:1]
	// and [index:4,term:4]. So 1 get the proposal[index:4,term:4] when apply the log[index:2,term:2],
	// you will find that index and term are not equal both. Just not reply.

	// Notify the Stale command
	for len(d.proposals) > 0 {
		prop := d.proposals[0]
		if prop.index < index || (prop.index == index && prop.term != term) {
			NotifyStaleReq(d.Term(), prop.cb)
			d.proposals = d.proposals[1:]
		} else {
			break
		}
	}

	if len(d.proposals) > 0 {
		prop := d.proposals[0]
		// Correct proposal
		if prop.index == index && prop.term == term {
			d.proposals = d.proposals[1:]
			return prop
		}
	}
	// Further proposal
	return nil
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		//log.Errorf("Peer %d receive a propose, but it no leader.", d.PeerId())
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// TODO: Handle the Admin Request, it will not enclose normal requests and administrator request in a `msg`
	// Raft command can only send to leader
	//if d.RaftGroup.Raft.State != raft.StateLeader {
	//	cmdResp := &raft_cmdpb.RaftCmdResponse{
	//		Header: &raft_cmdpb.RaftResponseHeader{},
	//	}
	//	BindRespError(cmdResp, &util.ErrNotLeader{
	//		RegionId: d.regionId,
	//		Leader:   d.getPeerFromCache(d.RaftGroup.Raft.Lead),
	//	})
	//	cb.Done(cmdResp)
	//	return
	//}
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	} else {
		d.proposeNormalRequest(msg, cb)
	}
}

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	adminReq := msg.AdminRequest
	switch adminReq.CmdType {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		if d.RaftGroup.Raft.PendingConfIndex != raft.None {
			// pending conf index in process
			resp := &raft_cmdpb.RaftCmdResponse{
				Header:    nil,
				Responses: nil,
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
					ChangePeer: &raft_cmdpb.ChangePeerResponse{
						Region: d.Region(),
					},
				},
			}
			BindRespError(resp, &util.ErrStaleCommand{})
			cb.Done(resp)
			return
		}
		p := &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}
		cc := eraftpb.ConfChange{
			ChangeType: msg.AdminRequest.ChangePeer.ChangeType,
			NodeId:     msg.AdminRequest.ChangePeer.Peer.Id,
			Context:    nil,
		}
		ctx, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		cc.Context = ctx
		err = d.RaftGroup.ProposeConfChange(cc)
		if err == raft.ErrInTransferLeader {
			resp := &raft_cmdpb.RaftCmdResponse{
				Header:    nil,
				Responses: nil,
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
					ChangePeer: &raft_cmdpb.ChangePeerResponse{
						Region: d.Region(),
					},
				},
			}
			BindRespError(resp, &util.ErrStaleCommand{})
			cb.Done(resp)
			return
		} else if err != nil {
			panic(err)
		}
		d.proposals = append(d.proposals, p)
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := proto.Marshal(msg)
		if err != nil {
			panic(err)
		}
		p := &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}
		err = d.RaftGroup.Propose(data)
		if err == raft.ErrInTransferLeader {
			// return stale command
			resp := &raft_cmdpb.RaftCmdResponse{
				Header:        nil,
				Responses:     nil,
				AdminResponse: nil,
			}
			BindRespError(resp, &util.ErrStaleCommand{})
			p.cb.Done(resp)
			return
		} else if err != nil {
			panic(err)
		}
		d.proposals = append(d.proposals, p)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		log.Infof("[Region: %d] Peer %d Try transfer leader to %d", d.regionId, d.PeerId(), msg.AdminRequest.TransferLeader.Peer.GetId())
		d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.GetId())
		cb.Done(d.newTransferLeaderResponse())
		return
	case raft_cmdpb.AdminCmdType_Split:
		// check key in region?
		splitReq := adminReq.Split
		err := util.CheckKeyInRegion(splitReq.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		// Marshal it
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		// prepare propose
		p := &proposal{
			index: d.nextProposalIndex(),
			term:  d.Term(),
			cb:    cb,
		}
		err = d.RaftGroup.Propose(data)
		if err == raft.ErrInTransferLeader {
			// return stale command
			resp := &raft_cmdpb.RaftCmdResponse{
				Header:        nil,
				Responses:     nil,
				AdminResponse: nil,
			}
			BindRespError(resp, &util.ErrStaleCommand{})
			p.cb.Done(resp)
			return
		} else if err != nil {
			panic(err)
		}
		d.proposals = append(d.proposals, p)
	}
}

func (d *peerMsgHandler) proposeNormalRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	p := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	err = d.RaftGroup.Propose(data)
	if err == raft.ErrInTransferLeader {
		// return stale command
		resp := &raft_cmdpb.RaftCmdResponse{
			Header:        nil,
			Responses:     nil,
			AdminResponse: nil,
		}
		BindRespError(resp, &util.ErrStaleCommand{})
		p.cb.Done(resp)
		return
	}
	d.proposals = append(d.proposals, p)

}
func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

func getRequestKey(req *raft_cmdpb.Request) []byte {
	var key []byte = nil
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	}
	return key
}

func (d *peerMsgHandler) newTransferLeaderResponse() *raft_cmdpb.RaftCmdResponse {
	head := &raft_cmdpb.RaftResponseHeader{
		Error:       nil,
		Uuid:        nil,
		CurrentTerm: d.Term(),
	}
	cmdResp := &raft_cmdpb.RaftCmdResponse{
		Header: head,
		AdminResponse: &raft_cmdpb.AdminResponse{
			CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
			TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
		},
	}
	return cmdResp
}

func (d *peerMsgHandler) getKey(msg *raft_cmdpb.RaftCmdRequest) []byte {
	if len(msg.Requests) != 0 {
		req := msg.Requests[0]
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			return req.Put.Key
		case raft_cmdpb.CmdType_Get:
			return req.Get.Key
		case raft_cmdpb.CmdType_Delete:
			return req.Delete.Key
		}
	} else if msg.AdminRequest != nil {
		req := msg.AdminRequest
		switch req.CmdType {
		case raft_cmdpb.AdminCmdType_Split:
			return req.Split.SplitKey
		}
	}
	return nil
}
