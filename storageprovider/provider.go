package storageprovider

// this file implements storagemarket.StorageProviderNode

import (
	"context"
	"github.com/filecoin-project/venus-market/api/clients"
	types3 "github.com/filecoin-project/venus-messager/types"
	"github.com/filecoin-project/venus/app/submodule/apitypes"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"

	"github.com/filecoin-project/venus/app/client/apiface"
	"github.com/filecoin-project/venus/pkg/constants"
	vCrypto "github.com/filecoin-project/venus/pkg/crypto"
	"github.com/filecoin-project/venus/pkg/events"
	"github.com/filecoin-project/venus/pkg/events/state"
	"github.com/filecoin-project/venus/pkg/types"
	"github.com/filecoin-project/venus/pkg/types/specactors/builtin/market"
	"github.com/filecoin-project/venus/pkg/types/specactors/builtin/miner"
	"github.com/filecoin-project/venus/pkg/wallet"

	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/fundmgr"
	types2 "github.com/filecoin-project/venus-market/types"
	"github.com/filecoin-project/venus-market/utils"
	"github.com/ipfs-force-community/venus-common-utils/metrics"
)

var defaultMaxProviderCollateralMultiplier = uint64(2)
var log = logging.Logger("storageadapter")

type ProviderNodeAdapter struct {
	apiface.FullNode

	fundMgr   *fundmgr.FundManager
	msgClient clients.IMixMessage
	ev        *events.Events

	dealPublisher *DealPublisher

	extendPieceMeta             DealAssiger
	addBalanceSpec              *types3.MsgMeta
	maxDealCollateralMultiplier uint64
	dsMatcher                   *dealStateMatcher
	scMgr                       *SectorCommittedManager
}

func NewProviderNodeAdapter(fc *config.MarketConfig) func(mctx metrics.MetricsCtx, lc fx.Lifecycle, node apiface.FullNode, msgClient clients.IMixMessage, dealPublisher *DealPublisher, fundMgr *fundmgr.FundManager, extendPieceMeta DealAssiger) StorageProviderNode {
	return func(mctx metrics.MetricsCtx, lc fx.Lifecycle, full apiface.FullNode, msgClient clients.IMixMessage, dealPublisher *DealPublisher, fundMgr *fundmgr.FundManager, extendPieceMeta DealAssiger) StorageProviderNode {
		ctx := metrics.LifecycleCtx(mctx, lc)

		ev, err := events.NewEvents(ctx, full)
		if err != nil {
			//todo add error return
			log.Warn(err)
		}
		na := &ProviderNodeAdapter{
			FullNode:        full,
			msgClient:       msgClient,
			ev:              ev,
			dealPublisher:   dealPublisher,
			dsMatcher:       newDealStateMatcher(state.NewStatePredicates(state.WrapFastAPI(full))),
			extendPieceMeta: extendPieceMeta,
			fundMgr:         fundMgr,
		}
		if fc != nil {
			na.addBalanceSpec = &types3.MsgMeta{MaxFee: abi.TokenAmount(fc.MaxMarketBalanceAddFee)}
			na.maxDealCollateralMultiplier = fc.MaxProviderCollateralMultiplier
		}
		na.maxDealCollateralMultiplier = defaultMaxProviderCollateralMultiplier
		na.scMgr = NewSectorCommittedManager(ev, na, &apiWrapper{api: full})
		return na
	}
}

func (n *ProviderNodeAdapter) PublishDeals(ctx context.Context, deal types2.MinerDeal) (cid.Cid, error) {
	return n.dealPublisher.Publish(ctx, deal.ClientDealProposal)
}

func (n *ProviderNodeAdapter) VerifySignature(ctx context.Context, sig crypto.Signature, addr address.Address, input []byte, encodedTs shared.TipSetToken) (bool, error) {
	addr, err := n.StateAccountKey(ctx, addr, types.EmptyTSK)
	if err != nil {
		return false, err
	}

	err = vCrypto.Verify(&sig, addr, input)
	return err == nil, err
}

func (n *ProviderNodeAdapter) GetMinerWorkerAddress(ctx context.Context, maddr address.Address, tok shared.TipSetToken) (address.Address, error) {
	tsk, err := types.TipSetKeyFromBytes(tok)
	if err != nil {
		return address.Undef, err
	}

	mi, err := n.StateMinerInfo(ctx, maddr, tsk)
	if err != nil {
		return address.Address{}, err
	}
	return mi.Worker, nil
}

func (n *ProviderNodeAdapter) GetProofType(ctx context.Context, maddr address.Address, tok shared.TipSetToken) (abi.RegisteredSealProof, error) {
	tsk, err := types.TipSetKeyFromBytes(tok)
	if err != nil {
		return 0, err
	}

	mi, err := n.StateMinerInfo(ctx, maddr, tsk)
	if err != nil {
		return 0, err
	}

	nver, err := n.StateNetworkVersion(ctx, tsk)
	if err != nil {
		return 0, err
	}

	return miner.PreferredSealProofTypeFromWindowPoStType(nver, mi.WindowPoStProofType)
}

func (n *ProviderNodeAdapter) Sign(ctx context.Context, data interface{}) (*crypto.Signature, error) {
	tok, _, err := n.GetChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("couldn't get chain head: %w", err)
	}

	switch data.(type) {
	case *types2.SignInfo:

	default:
		return nil, xerrors.Errorf("data type is not SignInfo")
	}

	info := data.(*types2.SignInfo)
	msgBytes, err := cborutil.Dump(info.Data)
	if err != nil {
		return nil, xerrors.Errorf("serializing: %w", err)
	}

	worker, err := n.GetMinerWorkerAddress(ctx, info.Addr, tok)
	if err != nil {
		return nil, err
	}

	signer, err := n.StateAccountKey(ctx, worker, types.EmptyTSK)
	if err != nil {
		return nil, err
	}
	localSignature, err := n.WalletSign(ctx, signer, msgBytes, wallet.MsgMeta{
		Type: info.Type,
	})
	if err != nil {
		return nil, err
	}
	return localSignature, nil
}

//
func (n *ProviderNodeAdapter) SignWithGivenMiner(mAddr address.Address) network.ResigningFunc {
	return func(ctx context.Context, data interface{}) (*crypto.Signature, error) {
		tok, _, err := n.GetChainHead(ctx)
		if err != nil {
			return nil, xerrors.Errorf("couldn't get chain head: %w", err)
		}

		msgBytes, err := cborutil.Dump(data)
		if err != nil {
			return nil, xerrors.Errorf("serializing: %w", err)
		}

		worker, err := n.GetMinerWorkerAddress(ctx, mAddr, tok)
		if err != nil {
			return nil, err
		}

		signer, err := n.StateAccountKey(ctx, worker, types.EmptyTSK)
		if err != nil {
			return nil, err
		}
		localSignature, err := n.WalletSign(ctx, signer, msgBytes, wallet.MsgMeta{
			Type: wallet.MTUnknown,
		})
		if err != nil {
			return nil, err
		}
		return localSignature, nil
	}
}

func (n *ProviderNodeAdapter) ReserveFunds(ctx context.Context, wallet, addr address.Address, amt abi.TokenAmount) (cid.Cid, error) {
	return n.fundMgr.Reserve(ctx, wallet, addr, amt)
}

func (n *ProviderNodeAdapter) ReleaseFunds(ctx context.Context, addr address.Address, amt abi.TokenAmount) error {
	return n.fundMgr.Release(addr, amt)
}

// Adds funds with the StorageMinerActor for a piecestorage participant.  Used by both providers and clients.
func (n *ProviderNodeAdapter) AddFunds(ctx context.Context, addr address.Address, amount abi.TokenAmount) (cid.Cid, error) {
	// (Provider Node API)
	msgId, err := n.msgClient.PushMessage(ctx, &types.Message{
		To:     market.Address,
		From:   addr,
		Value:  amount,
		Method: market.Methods.AddBalance,
	}, n.addBalanceSpec)
	if err != nil {
		return cid.Undef, err
	}

	return msgId, nil
}

func (n *ProviderNodeAdapter) GetBalance(ctx context.Context, addr address.Address, encodedTs shared.TipSetToken) (storagemarket.Balance, error) {
	tsk, err := types.TipSetKeyFromBytes(encodedTs)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	bal, err := n.StateMarketBalance(ctx, addr, tsk)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	return utils.ToSharedBalance(bal), nil
}

func (n *ProviderNodeAdapter) DealProviderCollateralBounds(ctx context.Context, size abi.PaddedPieceSize, isVerified bool) (abi.TokenAmount, abi.TokenAmount, error) {
	bounds, err := n.StateDealProviderCollateralBounds(ctx, size, isVerified, types.EmptyTSK)
	if err != nil {
		return abi.TokenAmount{}, abi.TokenAmount{}, err
	}

	// The maximum amount of collateral that the provider will put into escrow
	// for a deal is calculated as a multiple of the minimum bounded amount
	max := types.BigMul(bounds.Min, types.NewInt(n.maxDealCollateralMultiplier))

	return bounds.Min, max, nil
}

// TODO: Remove dealID parameter, change publishCid to be cid.Cid (instead of pointer)
func (n *ProviderNodeAdapter) OnDealSectorPreCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, proposal market2.DealProposal, publishCid *cid.Cid, cb storagemarket.DealSectorPreCommittedCallback) error {
	return n.scMgr.OnDealSectorPreCommitted(ctx, provider, market.DealProposal(proposal), *publishCid, cb)
}

// TODO: Remove dealID parameter, change publishCid to be cid.Cid (instead of pointer)
func (n *ProviderNodeAdapter) OnDealSectorCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, sectorNumber abi.SectorNumber, proposal market2.DealProposal, publishCid *cid.Cid, cb storagemarket.DealSectorCommittedCallback) error {
	return n.scMgr.OnDealSectorCommitted(ctx, provider, sectorNumber, market.DealProposal(proposal), *publishCid, func(err error) {
		cb(err)
		_Err := n.extendPieceMeta.UpdateDealStatus(ctx, provider, dealID, "Proving")
		if _Err != nil {
			log.Errorw("update deal status %w", _Err)
		}
	})
}

func (n *ProviderNodeAdapter) GetChainHead(ctx context.Context) (shared.TipSetToken, abi.ChainEpoch, error) {
	head, err := n.ChainHead(ctx)
	if err != nil {
		return nil, 0, err
	}

	return head.Key().Bytes(), head.Height(), nil
}

func (n *ProviderNodeAdapter) WaitForMessage(ctx context.Context, mcid cid.Cid, cb func(code exitcode.ExitCode, bytes []byte, finalCid cid.Cid, err error) error) error {
	receipt, err := n.msgClient.WaitMsg(ctx, mcid, 2*constants.MessageConfidence, constants.LookbackNoLimit, true)
	if err != nil {
		return cb(0, nil, cid.Undef, err)
	}
	ctx.Done()
	return cb(receipt.Receipt.ExitCode, receipt.Receipt.ReturnValue, receipt.Message, nil)
}

func (n *ProviderNodeAdapter) WaitForPublishDeals(ctx context.Context, publishCid cid.Cid, proposal market2.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
	// Wait for deal to be published (plus additional time for confidence)
	receipt, err := n.msgClient.WaitMsg(ctx, publishCid, 2*constants.MessageConfidence, constants.LookbackNoLimit, true)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals errored: %w", err)
	}
	if receipt.Receipt.ExitCode != exitcode.Ok {
		return nil, xerrors.Errorf("WaitForPublishDeals exit code: %s", receipt.Receipt.ExitCode)
	}

	// The deal ID may have changed since publish if there was a reorg, so
	// get the current deal ID
	head, err := n.ChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals failed to get chain head: %w", err)
	}

	res, err := n.scMgr.dealInfo.GetCurrentDealInfo(ctx, head.Key(), (*market.DealProposal)(&proposal), publishCid)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals getting deal info errored: %w", err)
	}

	return &storagemarket.PublishDealsWaitResult{DealID: res.DealID, FinalCid: receipt.Message}, nil
}

func (n *ProviderNodeAdapter) GetDataCap(ctx context.Context, addr address.Address, encodedTs shared.TipSetToken) (*abi.StoragePower, error) {
	tsk, err := types.TipSetKeyFromBytes(encodedTs)
	if err != nil {
		return nil, err
	}

	sp, err := n.StateVerifiedClientStatus(ctx, addr, tsk)
	return sp, err
}

func (n *ProviderNodeAdapter) OnDealExpiredOrSlashed(ctx context.Context, dealID abi.DealID, onDealExpired storagemarket.DealExpiredCallback, onDealSlashed storagemarket.DealSlashedCallback) error {
	head, err := n.ChainHead(ctx)
	if err != nil {
		return xerrors.Errorf("client: failed to get chain head: %w", err)
	}

	sd, err := n.StateMarketStorageDeal(ctx, dealID, head.Key())
	if err != nil {
		return xerrors.Errorf("client: failed to look up deal %d on chain: %w", dealID, err)
	}

	// Called immediately to check if the deal has already expired or been slashed
	checkFunc := func(ctx context.Context, ts *types.TipSet) (done bool, more bool, err error) {
		if ts == nil {
			// keep listening for events
			return false, true, nil
		}

		// Check if the deal has already expired
		if sd.Proposal.EndEpoch <= ts.Height() {
			onDealExpired(nil)
			return true, false, nil
		}

		// If there is no deal assume it's already been slashed
		if sd.State.SectorStartEpoch < 0 {
			onDealSlashed(ts.Height(), nil)
			return true, false, nil
		}

		// No events have occurred yet, so return
		// done: false, more: true (keep listening for events)
		return false, true, nil
	}

	// Called when there was a match against the state change we're looking for
	// and the chain has advanced to the confidence height
	stateChanged := func(ts *types.TipSet, ts2 *types.TipSet, states events.StateChange, h abi.ChainEpoch) (more bool, err error) {
		// Check if the deal has already expired
		if ts2 == nil || sd.Proposal.EndEpoch <= ts2.Height() {
			onDealExpired(nil)
			return false, nil
		}

		// Timeout waiting for state change
		if states == nil {
			log.Error("timed out waiting for deal expiry")
			return false, nil
		}

		changedDeals, ok := states.(state.ChangedDeals)
		if !ok {
			panic("Expected state.ChangedDeals")
		}

		deal, ok := changedDeals[dealID]
		if !ok {
			// No change to deal
			return true, nil
		}

		// Deal was slashed
		if deal.To == nil {
			onDealSlashed(ts2.Height(), nil)
			return false, nil
		}

		return true, nil
	}

	// Called when there was a chain reorg and the state change was reverted
	revert := func(ctx context.Context, ts *types.TipSet) error {
		// TODO: Is it ok to just ignore this?
		log.Warn("deal state reverted; TODO: actually handle this!")
		return nil
	}

	// Watch for state changes to the deal
	match := n.dsMatcher.matcher(ctx, dealID)

	// Wait until after the end epoch for the deal and then timeout
	timeout := (sd.Proposal.EndEpoch - head.Height()) + 1
	if err := n.ev.StateChanged(checkFunc, stateChanged, revert, int(constants.MessageConfidence)+1, timeout, match); err != nil {
		return xerrors.Errorf("failed to set up state changed handler: %w", err)
	}

	return nil
}

func (n *ProviderNodeAdapter) SearchMsg(ctx context.Context, from types.TipSetKey, msg cid.Cid, limit abi.ChainEpoch, allowReplaced bool) (*apitypes.MsgLookup, error) {
	return n.msgClient.WaitMsg(ctx, msg, constants.MessageConfidence, limit, allowReplaced)
}

func (n *ProviderNodeAdapter) GetMessage(ctx context.Context, mc cid.Cid) (*types.Message, error) {
	return n.msgClient.GetMessage(ctx, mc)
}

// StorageProviderNode are common interfaces provided by a filecoin Node to both StorageClient and StorageProvider
type StorageProviderNode interface {
	// Sign sign the given data with the given address's private key
	Sign(ctx context.Context, data interface{}) (*crypto.Signature, error)

	// SignWithGivenMiner sign the data with the worker address of the given miner
	SignWithGivenMiner(mAddr address.Address) network.ResigningFunc

	// GetChainHead returns a tipset token for the current chain head
	GetChainHead(ctx context.Context) (shared.TipSetToken, abi.ChainEpoch, error)

	// Adds funds with the StorageMinerActor for a storage participant.  Used by both providers and clients.
	AddFunds(ctx context.Context, addr address.Address, amount abi.TokenAmount) (cid.Cid, error)

	// ReserveFunds reserves the given amount of funds is ensures it is available for the deal
	ReserveFunds(ctx context.Context, wallet, addr address.Address, amt abi.TokenAmount) (cid.Cid, error)

	// ReleaseFunds releases funds reserved with ReserveFunds
	ReleaseFunds(ctx context.Context, addr address.Address, amt abi.TokenAmount) error

	// GetBalance returns locked/unlocked for a storage participant.  Used by both providers and clients.
	GetBalance(ctx context.Context, addr address.Address, tok shared.TipSetToken) (storagemarket.Balance, error)

	// VerifySignature verifies a given set of data was signed properly by a given address's private key
	VerifySignature(ctx context.Context, signature crypto.Signature, signer address.Address, plaintext []byte, tok shared.TipSetToken) (bool, error)

	// WaitForMessage waits until a message appears on chain. If it is already on chain, the callback is called immediately
	WaitForMessage(ctx context.Context, mcid cid.Cid, onCompletion func(exitcode.ExitCode, []byte, cid.Cid, error) error) error

	// DealProviderCollateralBounds returns the min and max collateral a storage provider can issue.
	DealProviderCollateralBounds(ctx context.Context, size abi.PaddedPieceSize, isVerified bool) (abi.TokenAmount, abi.TokenAmount, error)

	// OnDealSectorPreCommitted waits for a deal's sector to be pre-committed
	OnDealSectorPreCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, proposal market2.DealProposal, publishCid *cid.Cid, cb storagemarket.DealSectorPreCommittedCallback) error

	// OnDealSectorCommitted waits for a deal's sector to be sealed and proved, indicating the deal is active
	OnDealSectorCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, sectorNumber abi.SectorNumber, proposal market2.DealProposal, publishCid *cid.Cid, cb storagemarket.DealSectorCommittedCallback) error

	// OnDealExpiredOrSlashed registers callbacks to be called when the deal expires or is slashed
	OnDealExpiredOrSlashed(ctx context.Context, dealID abi.DealID, onDealExpired storagemarket.DealExpiredCallback, onDealSlashed storagemarket.DealSlashedCallback) error

	// PublishDeals publishes a deal on chain, returns the message cid, but does not wait for message to appear
	PublishDeals(ctx context.Context, deal types2.MinerDeal) (cid.Cid, error)

	// WaitForPublishDeals waits for a deal publish message to land on chain.
	WaitForPublishDeals(ctx context.Context, mcid cid.Cid, proposal market2.DealProposal) (*storagemarket.PublishDealsWaitResult, error)

	// GetMinerWorkerAddress returns the worker address associated with a miner
	GetMinerWorkerAddress(ctx context.Context, addr address.Address, tok shared.TipSetToken) (address.Address, error)

	// GetDataCap gets the current data cap for addr
	GetDataCap(ctx context.Context, addr address.Address, tok shared.TipSetToken) (*abi.StoragePower, error)

	// GetProofType gets the current seal proof type for the given miner.
	GetProofType(ctx context.Context, addr address.Address, tok shared.TipSetToken) (abi.RegisteredSealProof, error)
}

var _ StorageProviderNode = &ProviderNodeAdapter{}