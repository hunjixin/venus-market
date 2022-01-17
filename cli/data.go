package cli

import (
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var ExportCmds = &cli.Command{
	Name:      "export",
	Usage:     "export data(deals, ask, voucher)",
	ArgsUsage: "<path>",
	Action: func(cctx *cli.Context) error {
		api, closer, err := NewMarketNode(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := ReqContext(cctx)
		if cctx.NArg() == 0 {
			return xerrors.New("must specify path to storage data")
		}
		return api.ExportData(ctx, cctx.Args().Get(0))
	},
}
