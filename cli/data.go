package cli

import (
	"fmt"
	"github.com/urfave/cli/v2"
)

var ImportCmds = &cli.Command{
	Name:      "import",
	Usage:     "import v1 data to v2",
	ArgsUsage: "<import file path>",
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := NewMarketNode(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := ReqContext(cctx)

		pieceCids, err := nodeApi.PiecesListPieces(ctx)
		if err != nil {
			return err
		}

		for _, pc := range pieceCids {
			fmt.Println(pc)
		}
		return nil
	},
}
