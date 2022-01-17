package main

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/venus-market/api"
	"github.com/filecoin-project/venus-market/api/clients"
	"github.com/filecoin-project/venus-market/api/impl"
	"github.com/filecoin-project/venus-market/builder"
	cli2 "github.com/filecoin-project/venus-market/cli"
	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/fundmgr"
	"github.com/filecoin-project/venus-market/journal"
	"github.com/filecoin-project/venus-market/metrics"
	"github.com/filecoin-project/venus-market/models"
	"github.com/filecoin-project/venus-market/network"
	"github.com/filecoin-project/venus-market/paychmgr"
	"github.com/filecoin-project/venus-market/piece"
	"github.com/filecoin-project/venus-market/retrievaladapter"
	"github.com/filecoin-project/venus-market/rpc"
	"github.com/filecoin-project/venus-market/sealer"
	"github.com/filecoin-project/venus-market/storageadapter"
	"github.com/filecoin-project/venus-market/types"
	"github.com/filecoin-project/venus-market/utils"
	"github.com/filecoin-project/venus/pkg/constants"
	_ "github.com/filecoin-project/venus/pkg/crypto/bls"
	_ "github.com/filecoin-project/venus/pkg/crypto/secp"
	metrics2 "github.com/ipfs/go-metrics-interface"
	"github.com/urfave/cli/v2"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"log"
	"os"
)

// Invokes are called in the order they are defined.
//nolint:golint
var (
	InitJournalKey builder.Invoke = builder.NextInvoke() //nolint
	ExtractApiKey  builder.Invoke = builder.NextInvoke()
)

var (
	RepoFlag = &cli.StringFlag{
		Name:    "repo",
		EnvVars: []string{"VENUS_MARKET_PATH"},
		Value:   "~/.venusmarket",
	}

	NodeUrlFlag = &cli.StringFlag{
		Name:  "node-url",
		Usage: "url to connect to daemon service",
	}

	MessagerUrlFlag = &cli.StringFlag{
		Name:  "messager-url",
		Usage: "url to connect messager service",
	}

	AuthTokeFlag = &cli.StringFlag{
		Name:  "auth-token",
		Usage: "token for connect venus componets",
	}

	SignerUrlFlag = &cli.StringFlag{
		Name:  "signer-url",
		Usage: "used to connect signer service for sign",
	}
	SignerTokenFlag = &cli.StringFlag{
		Name:  "signer-token",
		Usage: "auth token for connect signer service",
	}

	MinerFlag = &cli.StringFlag{
		Name:  "miner",
		Usage: "miner address",
	}

	PieceStorageFlag = &cli.StringFlag{
		Name:  "piecestorage",
		Usage: "config storage for piece",
	}

	TransferPathFlag = &cli.StringFlag{
		Name:  "transfer-path",
		Usage: "data transfer temporary data storage path",
	}
)

func main() {
	app := &cli.App{
		Name:                 "venus-market",
		Usage:                "venus-market",
		Version:              constants.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			RepoFlag,
		},
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "run market daemon",
				Flags: []cli.Flag{
					NodeUrlFlag,
					MessagerUrlFlag,
					AuthTokeFlag,
					SignerUrlFlag,
					SignerTokenFlag,
					PieceStorageFlag,
					TransferPathFlag,
					MinerFlag,
				},
				Action: daemon,
			},
			cli2.PiecesCmd,
			cli2.RetrievalDealsCmd,
			cli2.StorageDealsCmd,
			cli2.ActorCmd,
			cli2.NetCmd,
			cli2.DataTransfersCmd,
			cli2.DagstoreCmd,
			cli2.ExportCmds,
		},
	}

	app.Setup()
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func prepare(cctx *cli.Context) (*config.MarketConfig, error) {
	cfg := config.DefaultMarketConfig
	cfg.HomeDir = cctx.String("repo")
	cfgPath, err := cfg.ConfigPath()
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(cfgPath); os.IsNotExist(err) {
		//create
		err = flagData(cctx, cfg)
		if err != nil {
			return nil, xerrors.Errorf("parser data from flag %w", err)
		}

		err = config.SaveConfig(cfg)
		if err != nil {
			return nil, xerrors.Errorf("save config to %s %w", cfgPath, err)
		}
	} else if err == nil {
		//loadConfig
		err = config.LoadConfig(cfgPath, cfg)
		if err != nil {
			return nil, err
		}
		err = flagData(cctx, cfg)
		if err != nil {
			return nil, xerrors.Errorf("parser data from flag %w", err)
		}
	} else {
		return nil, err
	}
	return cfg, nil
}

func daemon(cctx *cli.Context) error {
	utils.SetupLogLevels()
	ctx := cctx.Context
	cfg, err := prepare(cctx)
	if err != nil {
		return err
	}

	resAPI := &impl.MarketNodeImpl{}
	shutdownChan := make(chan struct{})
	_, err = builder.New(ctx,
		//defaults
		builder.Override(new(journal.DisabledEvents), journal.EnvDisabledEvents),
		builder.Override(new(journal.Journal), journal.OpenFilesystemJournal),

		builder.Override(new(metrics.MetricsCtx), func() context.Context {
			return metrics2.CtxScope(context.Background(), "venus-market")
		}),
		builder.Override(new(types.ShutdownChan), shutdownChan),
		//config
		config.ConfigServerOpts(cfg),
		//clients
		clients.ClientsOpts(true, &cfg.Messager, &cfg.Signer),

		models.DBOptions(true),
		network.NetworkOpts(true, cfg.SimultaneousTransfers),
		piece.PieceOpts(cfg),
		fundmgr.FundMgrOpts,
		sealer.SealerOpts,
		paychmgr.PaychOpts,
		// Markets
		storageadapter.StorageProviderOpts(cfg),
		retrievaladapter.RetrievalProviderOpts(cfg),

		func(s *builder.Settings) error {
			s.Invokes[ExtractApiKey] = builder.InvokeOption{
				Priority: 10,
				Option:   fx.Populate(resAPI),
			}
			return nil
		},
	)
	if err != nil {
		return xerrors.Errorf("initializing node: %w", err)
	}
	finishCh := utils.MonitorShutdown(shutdownChan)

	return rpc.ServeRPC(ctx, cfg, &cfg.API, api.MarketFullNode(resAPI), finishCh, 1000, "")
}

func flagData(cctx *cli.Context, cfg *config.MarketConfig) error {
	if cctx.IsSet("repo") {
		cfg.HomeDir = cctx.String("repo")
	}
	if cctx.IsSet("node-url") {
		cfg.Node.Url = cctx.String("node-url")
	}
	if cctx.IsSet("auth-token") {
		cfg.Node.Token = cctx.String("auth-token")
	}

	if cctx.IsSet("messager-url") {
		cfg.Messager.Url = cctx.String("messager-url")
	}
	if cctx.IsSet("auth-token") {
		cfg.Messager.Token = cctx.String("auth-token")
	}

	if cctx.IsSet("signer-url") {
		cfg.Signer.Url = cctx.String("signer-url")
	}
	if cctx.IsSet("signer-token") {
		cfg.Signer.Token = cctx.String("signer-token")
	}

	if cctx.IsSet("miner") {
		addr, err := address.NewFromString(cctx.String("miner"))
		if err != nil {
			return err
		}
		cfg.MinerAddress = addr.String()
	}

	if cctx.IsSet("piecestorage") {
		cfg.PieceStorage = config.PieceStorageString(cctx.String("piecestorage"))
	}

	if cctx.IsSet("transfer-path") {
		cfg.TransferPath = cctx.String("transfer-path")
	}

	return nil
}
