package paychmgr

import (
	"context"
	"errors"
	"sync"

	crypto2 "github.com/filecoin-project/venus/pkg/crypto"
	"github.com/filecoin-project/venus/venus-shared/actors/builtin/market"

	"github.com/filecoin-project/venus/pkg/constants"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/venus/venus-shared/actors/builtin/paych"
	"github.com/filecoin-project/venus/venus-shared/types"
)

type mockManagerAPI struct {
	*mockStateManager
	*mockPaychAPI
}

func newMockManagerAPI() *mockManagerAPI {
	return &mockManagerAPI{
		mockStateManager: newMockStateManager(),
		mockPaychAPI:     newMockPaychAPI(),
	}
}

type mockPchState struct {
	actor *types.Actor
	state paych.State
}

type mockStateManager struct {
	lk           sync.Mutex
	accountState map[address.Address]address.Address
	paychState   map[address.Address]mockPchState
	response     *types.InvocResult
	lastCall     *types.Message
}

func newMockStateManager() *mockStateManager {
	return &mockStateManager{
		accountState: make(map[address.Address]address.Address),
		paychState:   make(map[address.Address]mockPchState),
	}
}

func (sm *mockStateManager) setAccountAddress(a address.Address, lookup address.Address) {
	sm.lk.Lock()
	defer sm.lk.Unlock()
	sm.accountState[a] = lookup
}

func (sm *mockStateManager) setPaychState(a address.Address, actor *types.Actor, state paych.State) {
	sm.lk.Lock()
	defer sm.lk.Unlock()
	sm.paychState[a] = mockPchState{actor, state}
}

func (sm *mockStateManager) resolveToKeyAddress(ctx context.Context, addr address.Address, ts *types.TipSet) (address.Address, error) {
	sm.lk.Lock()
	defer sm.lk.Unlock()
	keyAddr, ok := sm.accountState[addr]
	if !ok {
		return address.Undef, errors.New("not found")
	}
	return keyAddr, nil
}

func (m mockManagerAPI) getMarketState(ctx context.Context, ts *types.TipSet) (market.State, error) {
	return nil, nil
}

func (sm *mockStateManager) getPaychState(ctx context.Context, addr address.Address, ts *types.TipSet) (*types.Actor, paych.State, error) {
	sm.lk.Lock()
	defer sm.lk.Unlock()
	info, ok := sm.paychState[addr]
	if !ok {
		return nil, nil, errors.New("not found")
	}
	return info.actor, info.state, nil
}

func (sm *mockStateManager) setCallResponse(response *types.InvocResult) {
	sm.lk.Lock()
	defer sm.lk.Unlock()

	sm.response = response
}

func (sm *mockStateManager) getLastCall() *types.Message {
	sm.lk.Lock()
	defer sm.lk.Unlock()

	return sm.lastCall
}

func (sm *mockStateManager) call(ctx context.Context, msg *types.Message, ts *types.TipSet) (*types.InvocResult, error) {
	sm.lk.Lock()
	defer sm.lk.Unlock()

	sm.lastCall = msg

	return sm.response, nil
}

type waitingCall struct {
	response chan types.MessageReceipt
}

type waitingResponse struct {
	receipt types.MessageReceipt
	done    chan struct{}
}

type mockPaychAPI struct {
	lk               sync.Mutex
	messages         map[cid.Cid]*types.SignedMessage
	waitingCalls     map[cid.Cid]*waitingCall
	waitingResponses map[cid.Cid]*waitingResponse
	wallet           map[address.Address]struct{}
	signingKey       []byte
}

func newMockPaychAPI() *mockPaychAPI {
	return &mockPaychAPI{
		messages:         make(map[cid.Cid]*types.SignedMessage),
		waitingCalls:     make(map[cid.Cid]*waitingCall),
		waitingResponses: make(map[cid.Cid]*waitingResponse),
		wallet:           make(map[address.Address]struct{}),
	}
}

func (pchapi *mockPaychAPI) WaitMsg(ctx context.Context, mcid cid.Cid, confidence uint64) (*types.MsgLookup, error) {
	pchapi.lk.Lock()
	response := make(chan types.MessageReceipt)

	if response, ok := pchapi.waitingResponses[mcid]; ok {
		defer pchapi.lk.Unlock()
		defer func() {
			go close(response.done)
		}()

		delete(pchapi.waitingResponses, mcid)
		return &types.MsgLookup{Receipt: response.receipt}, nil
	}

	pchapi.waitingCalls[mcid] = &waitingCall{response: response}
	pchapi.lk.Unlock()

	receipt := <-response
	return &types.MsgLookup{Receipt: receipt}, nil
}

func (pchapi *mockPaychAPI) receiveMsgResponse(mcid cid.Cid, receipt types.MessageReceipt) {
	pchapi.lk.Lock()

	if call, ok := pchapi.waitingCalls[mcid]; ok {
		defer pchapi.lk.Unlock()

		delete(pchapi.waitingCalls, mcid)
		call.response <- receipt
		return
	}

	done := make(chan struct{})
	pchapi.waitingResponses[mcid] = &waitingResponse{receipt: receipt, done: done}

	pchapi.lk.Unlock()

	<-done
}

// Send success response for any waiting calls
func (pchapi *mockPaychAPI) close() {
	pchapi.lk.Lock()
	defer pchapi.lk.Unlock()

	success := types.MessageReceipt{
		ExitCode: 0,
		Return:   []byte{},
	}
	for mcid, call := range pchapi.waitingCalls {
		delete(pchapi.waitingCalls, mcid)
		call.response <- success
	}
}

func (pchapi *mockPaychAPI) PushMessage(ctx context.Context, msg *types.Message, spec *types.MessageSendSpec) (cid.Cid, error) {
	pchapi.lk.Lock()
	defer pchapi.lk.Unlock()

	smsg := &types.SignedMessage{Message: *msg}
	smsgCid := smsg.Cid()
	pchapi.messages[smsgCid] = smsg
	return smsgCid, nil
}

func (pchapi *mockPaychAPI) pushedMessages(c cid.Cid) *types.SignedMessage {
	pchapi.lk.Lock()
	defer pchapi.lk.Unlock()

	return pchapi.messages[c]
}

func (pchapi *mockPaychAPI) pushedMessageCount() int {
	pchapi.lk.Lock()
	defer pchapi.lk.Unlock()

	return len(pchapi.messages)
}

func (pchapi *mockPaychAPI) StateAccountKey(ctx context.Context, addr address.Address, tsk types.TipSetKey) (address.Address, error) {
	return addr, nil
}

func (pchapi *mockPaychAPI) WalletHas(ctx context.Context, addr address.Address) (bool, error) {
	pchapi.lk.Lock()
	defer pchapi.lk.Unlock()

	_, ok := pchapi.wallet[addr]
	return ok, nil
}

func (pchapi *mockPaychAPI) addWalletAddress(addr address.Address) {
	pchapi.lk.Lock()
	defer pchapi.lk.Unlock()

	pchapi.wallet[addr] = struct{}{}
}

func (pchapi *mockPaychAPI) WalletSign(ctx context.Context, k address.Address, msg []byte) (*crypto.Signature, error) {
	pchapi.lk.Lock()
	defer pchapi.lk.Unlock()

	return crypto2.Sign(msg, pchapi.signingKey, crypto.SigTypeSecp256k1)
}

func (pchapi *mockPaychAPI) addSigningKey(key []byte) {
	pchapi.lk.Lock()
	defer pchapi.lk.Unlock()

	pchapi.signingKey = key
}

func (pchapi *mockPaychAPI) StateNetworkVersion(ctx context.Context, tsk types.TipSetKey) (network.Version, error) {
	return constants.NewestNetworkVersion, nil
}
