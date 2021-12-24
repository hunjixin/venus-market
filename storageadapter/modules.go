package storageadapter

import (
	"context"
	"fmt"
	"github.com/mitchellh/go-homedir"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/fx"

	"github.com/filecoin-project/go-address"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/network"
	dtgstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-fil-markets/filestore"
	piecefilestore "github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	storageimpl "github.com/filecoin-project/go-fil-markets/storagemarket/impl"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/storedask"
	smnet "github.com/filecoin-project/go-fil-markets/storagemarket/network"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"

	"github.com/filecoin-project/venus/app/client/apiface"
	"github.com/filecoin-project/venus/pkg/constants"
	"github.com/filecoin-project/venus/pkg/types"

	"github.com/filecoin-project/venus-market/builder"
	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/dagstore"
	"github.com/filecoin-project/venus-market/dealfilter"
	"github.com/filecoin-project/venus-market/journal"
	"github.com/filecoin-project/venus-market/metrics"
	"github.com/filecoin-project/venus-market/models"
	"github.com/filecoin-project/venus-market/network"
	types2 "github.com/filecoin-project/venus-market/types"
	"github.com/filecoin-project/venus-market/utils"
)

var (
	HandleDealsKey builder.Invoke = builder.NextInvoke()
)

func NewStorageAsk(ctx metrics.MetricsCtx,
	fapi apiface.FullNode,
	askDs models.StorageAskDS,
	minerAddress types2.MinerAddress,
	spn storagemarket.StorageProviderNode) (*storedask.StoredAsk, error) {

	fmt.Println(address.Address(minerAddress).String())
	mi, err := fapi.StateMinerInfo(ctx, address.Address(minerAddress), types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	return storedask.NewStoredAsk(askDs, datastore.NewKey("latest"), spn, address.Address(minerAddress),
		storagemarket.MaxPieceSize(abi.PaddedPieceSize(mi.SectorSize)))
}

func NewTransferStore(transferPath string) func() (filestore.FileStore, error) {
	return func() (filestore.FileStore, error) {
		path, err := homedir.Expand(transferPath)
		if err != nil {
			return nil, err
		}
		return piecefilestore.NewLocalFileStore(piecefilestore.OsPath(path))
	}
}

func StorageProvider(
	h host.Host,
	minerAddress types2.MinerAddress,
	storedAsk *storedask.StoredAsk,
	transferStore filestore.FileStore,
	providerDealsDs models.ProviderDealDS,
	dagStore *dagstore.Wrapper,
	pieceStore piecestore.PieceStore,
	dataTransfer network.ProviderDataTransfer,
	spn storagemarket.StorageProviderNode,
	df config.StorageDealFilter,
) (storagemarket.StorageProvider, error) {
	net := smnet.NewFromLibp2pHost(h)

	opt := storageimpl.CustomDealDecisionLogic(storageimpl.DealDeciderFunc(df))

	return storageimpl.NewProvider(net, providerDealsDs, transferStore, dagStore, pieceStore, dataTransfer, spn, address.Address(minerAddress), storedAsk, opt)
}

func HandleDeals(mctx metrics.MetricsCtx, lc fx.Lifecycle, host host.Host, h storagemarket.StorageProvider, j journal.Journal) {
	ctx := metrics.LifecycleCtx(mctx, lc)
	h.OnReady(utils.ReadyLogger("piecestorage provider"))
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			h.SubscribeToEvents(utils.StorageProviderLogger)

			evtType := j.RegisterEventType("markets/piecestorage/provider", "state_change")
			h.SubscribeToEvents(utils.StorageProviderJournaler(j, evtType))

			return h.Start(ctx)
		},
		OnStop: func(context.Context) error {
			return h.Stop()
		},
	})
}

// NewProviderDAGServiceDataTransfer returns a data transfer manager that just
// uses the provider's Staging DAG service for transfers
func NewProviderDAGServiceDataTransfer(lc fx.Lifecycle, dagDs models.DagTransferDS, h host.Host, homeDir *config.HomeDir, gs network.StagingGraphsync) (network.ProviderDataTransfer, error) {
	net := dtnet.NewFromLibp2pHost(h)

	transport := dtgstransport.NewTransport(h.ID(), gs)
	err := os.MkdirAll(filepath.Join(string(*homeDir), "data-transfer"), 0755) //nolint: gosec
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	dt, err := dtimpl.NewDataTransfer(dagDs, filepath.Join(string(*homeDir), "data-transfer"), net, transport)
	if err != nil {
		return nil, err
	}

	dt.OnReady(utils.ReadyLogger("provider data transfer"))
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			dt.SubscribeToEvents(utils.DataTransferLogger)
			return dt.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return dt.Stop(ctx)
		},
	})
	return dt, nil
}

func BasicDealFilter(user config.StorageDealFilter) func(onlineOk config.ConsiderOnlineStorageDealsConfigFunc,
	offlineOk config.ConsiderOfflineStorageDealsConfigFunc,
	verifiedOk config.ConsiderVerifiedStorageDealsConfigFunc,
	unverifiedOk config.ConsiderUnverifiedStorageDealsConfigFunc,
	blocklistFunc config.StorageDealPieceCidBlocklistConfigFunc,
	expectedSealTimeFunc config.GetExpectedSealDurationFunc,
	startDelay config.GetMaxDealStartDelayFunc,
	spn storagemarket.StorageProviderNode) config.StorageDealFilter {
	return func(onlineOk config.ConsiderOnlineStorageDealsConfigFunc,
		offlineOk config.ConsiderOfflineStorageDealsConfigFunc,
		verifiedOk config.ConsiderVerifiedStorageDealsConfigFunc,
		unverifiedOk config.ConsiderUnverifiedStorageDealsConfigFunc,
		blocklistFunc config.StorageDealPieceCidBlocklistConfigFunc,
		expectedSealTimeFunc config.GetExpectedSealDurationFunc,
		startDelay config.GetMaxDealStartDelayFunc,
		spn storagemarket.StorageProviderNode) config.StorageDealFilter {

		return func(ctx context.Context, deal storagemarket.MinerDeal) (bool, string, error) {
			b, err := onlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if deal.Ref != nil && deal.Ref.TransferType != storagemarket.TTManual && !b {
				log.Warnf("online piecestorage deal consideration disabled; rejecting piecestorage deal proposal from client: %s", deal.Client.String())
				return false, "miner is not considering online piecestorage deals", nil
			}

			b, err = offlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if deal.Ref != nil && deal.Ref.TransferType == storagemarket.TTManual && !b {
				log.Warnf("offline piecestorage deal consideration disabled; rejecting piecestorage deal proposal from client: %s", deal.Client.String())
				return false, "miner is not accepting offline piecestorage deals", nil
			}

			b, err = verifiedOk()
			if err != nil {
				return false, "miner error", err
			}

			if deal.Proposal.VerifiedDeal && !b {
				log.Warnf("verified piecestorage deal consideration disabled; rejecting piecestorage deal proposal from client: %s", deal.Client.String())
				return false, "miner is not accepting verified piecestorage deals", nil
			}

			b, err = unverifiedOk()
			if err != nil {
				return false, "miner error", err
			}

			if !deal.Proposal.VerifiedDeal && !b {
				log.Warnf("unverified piecestorage deal consideration disabled; rejecting piecestorage deal proposal from client: %s", deal.Client.String())
				return false, "miner is not accepting unverified piecestorage deals", nil
			}

			blocklist, err := blocklistFunc()
			if err != nil {
				return false, "miner error", err
			}

			for idx := range blocklist {
				if deal.Proposal.PieceCID.Equals(blocklist[idx]) {
					log.Warnf("piece CID in proposal %s is blocklisted; rejecting piecestorage deal proposal from client: %s", deal.Proposal.PieceCID, deal.Client.String())
					return false, fmt.Sprintf("miner has blocklisted piece CID %s", deal.Proposal.PieceCID), nil
				}
			}

			sealDuration, err := expectedSealTimeFunc()
			if err != nil {
				return false, "miner error", err
			}

			sealEpochs := sealDuration / (time.Duration(constants.MainNetBlockDelaySecs) * time.Second)
			_, ht, err := spn.GetChainHead(ctx)
			if err != nil {
				return false, "failed to get chain head", err
			}
			earliest := abi.ChainEpoch(sealEpochs) + ht
			if deal.Proposal.StartEpoch < earliest {
				log.Warnw("proposed deal would start before sealing can be completed; rejecting piecestorage deal proposal from client", "piece_cid", deal.Proposal.PieceCID, "client", deal.Client.String(), "seal_duration", sealDuration, "earliest", earliest, "curepoch", ht)
				return false, fmt.Sprintf("cannot seal a sector before %s", deal.Proposal.StartEpoch), nil
			}

			sd, err := startDelay()
			if err != nil {
				return false, "miner error", err
			}

			// Reject if it's more than 7 days in the future
			// TODO: read from cfg how to get block delay
			maxStartEpoch := earliest + abi.ChainEpoch(uint64(sd.Seconds())/constants.MainNetBlockDelaySecs)
			if deal.Proposal.StartEpoch > maxStartEpoch {
				return false, fmt.Sprintf("deal start epoch is too far in the future: %s > %s", deal.Proposal.StartEpoch, maxStartEpoch), nil
			}

			if user != nil {
				return user(ctx, deal)
			}

			return true, "", nil
		}
	}
}

var StorageProviderOpts = func(cfg *config.MarketConfig) builder.Option {
	return builder.Options(
		builder.Override(new(*storedask.StoredAsk), NewStorageAsk),
		builder.Override(new(network.ProviderDataTransfer), NewProviderDAGServiceDataTransfer), //save to metadata /datatransfer/provider/transfers
		//   save to metadata /deals/provider/piecestorage-ask/latest
		builder.Override(new(config.StorageDealFilter), BasicDealFilter(nil)),
		builder.Override(new(filestore.FileStore), NewTransferStore(cfg.TransferPath)),
		builder.Override(new(storagemarket.StorageProvider), StorageProvider),
		builder.Override(new(*DealPublisher), NewDealPublisher(cfg)),
		builder.Override(HandleDealsKey, HandleDeals),
		builder.Override(new(network.ProviderDataTransfer), NewProviderDAGServiceDataTransfer),
		builder.If(cfg.Filter != "",
			builder.Override(new(config.StorageDealFilter), BasicDealFilter(dealfilter.CliStorageDealFilter(cfg.Filter))),
		),
		builder.Override(new(*DealPublisher), NewDealPublisher(cfg)),
		builder.Override(new(storagemarket.StorageProviderNode), NewProviderNodeAdapter(cfg)),
	)
}

var StorageClientOpts = builder.Options(
	builder.Override(new(storagemarket.StorageClientNode), NewClientNodeAdapter),
)
