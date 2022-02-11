package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	versionedfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket/impl/providerstates"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statemachine/fsm"
	"github.com/filecoin-project/go-statestore"
	cli2 "github.com/filecoin-project/venus-market/cli"
	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/models"
	"github.com/filecoin-project/venus-market/piece"
	"github.com/filecoin-project/venus/pkg/constants"
	"github.com/filecoin-project/venus/pkg/paychmgr"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	badger "github.com/ipfs/go-ds-badger2"
	"github.com/urfave/cli/v2"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path"
)

var (
	RepoFlag = &cli.StringFlag{
		Name:    "repo",
		EnvVars: []string{"VENUS_MARKET_PATH"},
		Value:   "~/.venusmarket",
	}
)

func main() {
	app := &cli.App{
		Name:                 "venus-market-client",
		Usage:                "venus-market client",
		Version:              constants.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			RepoFlag,
		},
		Commands: append(cli2.ClientCmds, &cli.Command{
			Name:  "export",
			Usage: "export v1 data",
			Flags: []cli.Flag{},
			Action: func(c *cli.Context) error {
				ctx := context.Background()
				repo := c.String("repo")
				exportPath := c.Args().Get(0)
				return run(repo, exportPath, ctx)
			},
		}),
	}

	app.Setup()
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(repo string, dst string, ctx context.Context) error {
	cfgPath := path.Join(repo, "config.toml")
	cfg := config.DefaultMarketConfig
	err := config.LoadConfig(cfgPath, cfg)

	dbPath := path.Join(repo, "metadata")
	metadataDS, err := badger.NewDatastore(dbPath, &badger.DefaultOptions)
	if err != nil {
		return err
	}
	provierDs := models.NewProviderDealDS(metadataDS)
	storageAskDs := models.NewStorageAskDS(provierDs)
	minerDealsStore := statestore.New(namespace.Wrap(provierDs, datastore.NewKey(string("1"))))

	retrievalDS := models.NewRetrievalProviderDS(metadataDS)
	retrievalAskDs := models.NewRetrievalAskDS(retrievalDS)
	retrievalDealsStore := statestore.New(namespace.Wrap(retrievalDS, datastore.NewKey(string(versioning.VersionKey("1")))))

	payCh, err := paychmgr.NewManager(ctx, metadataDS, &paychmgr.ManagerParams{})
	if err != nil {
		return err
	}

	type minerDealsIncludeStatus struct {
		MinerDeal storagemarket.MinerDeal
		DealInfo  piecestore.DealInfo
		Status    string
	}
	type exportData struct {
		Miner          address.Address
		MinerDeals     []minerDealsIncludeStatus
		SignedVoucher  map[string]*paychmgr.ChannelInfo
		StorageAsk     *storagemarket.SignedStorageAsk
		RetrievalAsk   *retrievalmarket.Ask
		RetrievalDeals []retrievalmarket.ProviderDealState
	}

	mAddr, err := address.NewFromString(cfg.MinerAddress)
	if err != nil {
		return err
	}

	var minerDeals []storagemarket.MinerDeal
	if err := minerDealsStore.List(&minerDeals); err != nil {
		return err
	}

	var retrievalDeals []retrievalmarket.ProviderDealState
	if err := retrievalDealsStore.List(&retrievalDeals); err != nil {
		return err
	}

	//voucher
	channelAddrs, err := payCh.ListChannels()
	if err != nil {
		return err
	}

	voucherDetail := map[string]*paychmgr.ChannelInfo{}
	for _, p := range channelAddrs {
		channelInfo, err := payCh.GetChannelInfo(p)
		if err != nil {
			return err
		}
		voucherDetail[p.String()] = channelInfo
	}
	//storage ask
	askb, err := namespace.Wrap(storageAskDs, datastore.NewKey("1")).Get(datastore.NewKey("latest"))
	if err != nil {
		return fmt.Errorf("failed to load most recent ask from disk: %w", err)
	}

	var storageAsk storagemarket.SignedStorageAsk
	if err := cborutil.ReadCborRPC(bytes.NewReader(askb), &storageAsk); err != nil {
		return err
	}

	//retrieval ask
	raskb, err := namespace.Wrap(retrievalAskDs, datastore.NewKey("1")).Get(datastore.NewKey("latest"))
	if err != nil {
		return fmt.Errorf("failed to load most recent retrieval ask from disk: %w", err)
	}

	var retrievalAsk retrievalmarket.Ask
	if err := cborutil.ReadCborRPC(bytes.NewReader(raskb), &retrievalAsk); err != nil {
		return err
	}

	pieceStore, err := piece.NewDsPieceStore(models.NewPieceInfoDs(models.NewPieceMetaDs(metadataDS)), 0, nil)
	if err != nil {
		return err
	}

	dealInfos, err := pieceStore.GetDeals(0, math.MaxInt64)
	if err != nil {
		return err
	}

	dealsInfos := make(map[abi.DealID]*piece.DealInfo)
	for _, dealInfo := range dealInfos {
		if dealInfo.DealID > 0 {
			dealsInfos[dealInfo.DealID] = dealInfo
		}
	}
	var deals []minerDealsIncludeStatus
	for _, minerDeal := range minerDeals {
		minerDealIncludeStatus := minerDealsIncludeStatus{
			MinerDeal: minerDeal,
			DealInfo:  piecestore.DealInfo{},
			Status:    piece.Undefine,
		}
		deals = append(deals, minerDealIncludeStatus)
		if minerDeal.DealID > 0 {
			if val, ok := dealsInfos[minerDeal.DealID]; ok {
				minerDealIncludeStatus.DealInfo = val.DealInfo
				minerDealIncludeStatus.Status = val.Status
			}
		}
	}

	data := exportData{
		Miner:          mAddr,
		MinerDeals:     deals,
		SignedVoucher:  voucherDetail,
		StorageAsk:     &storageAsk,
		RetrievalAsk:   &retrievalAsk,
		RetrievalDeals: retrievalDeals,
	}

	exportDataBytes, err := json.Marshal(&data)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(dst, exportDataBytes, 0777)

}
func newProviderStateMachine(ds datastore.Batching, env fsm.Environment, notifier fsm.Notifier, storageMigrations versioning.VersionedMigrationList, target versioning.VersionKey) (fsm.Group, func(context.Context) error, error) {
	return versionedfsm.NewVersionedFSM(ds, fsm.Parameters{
		Environment:     env,
		StateType:       storagemarket.MinerDeal{},
		StateKeyField:   "State",
		Events:          providerstates.ProviderEvents,
		StateEntryFuncs: providerstates.ProviderStateEntryFuncs,
		FinalityStates:  providerstates.ProviderFinalityStates,
		Notifier:        notifier,
	}, storageMigrations, target)
}
