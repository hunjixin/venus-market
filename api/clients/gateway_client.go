package clients

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/minermgr"
	"github.com/filecoin-project/venus-messager/gateway"
	vCrypto "github.com/filecoin-project/venus/pkg/crypto"
	types2 "github.com/filecoin-project/venus/venus-shared/types"
	"github.com/ipfs-force-community/venus-common-utils/apiinfo"
	"github.com/ipfs-force-community/venus-common-utils/metrics"
)

func newGatewayWalletClient(mctx metrics.MetricsCtx, mgr minermgr.IAddrMgr, nodeCfg *config.Signer) (ISinger, jsonrpc.ClientCloser, error) {
	info := apiinfo.NewAPIInfo(nodeCfg.Url, nodeCfg.Token)
	dialAddr, err := info.DialArgs("v0")
	if err != nil {
		return nil, nil, err
	}

	var client gateway.WalletClient
	closer, err := jsonrpc.NewClient(mctx, dialAddr, "Gateway", &client, info.AuthHeader())

	return &GatewayClient{
		innerClient: client,
		importMgr:   mgr,
	}, closer, err
}

type GatewayClient struct {
	innerClient gateway.WalletClient
	importMgr   minermgr.IAddrMgr
}

func (gatewayClient *GatewayClient) WalletHas(ctx context.Context, addr address.Address) (bool, error) {
	account, err := gatewayClient.importMgr.GetAccount(ctx, addr)
	if err != nil {
		return false, err
	}
	return gatewayClient.innerClient.WalletHas(ctx, account, addr)
}

func (gatewayClient *GatewayClient) WalletSign(ctx context.Context, addr address.Address, msg []byte, meta types2.MsgMeta) (*vCrypto.Signature, error) {
	account, err := gatewayClient.importMgr.GetAccount(ctx, addr)
	if err != nil {
		return nil, err
	}

	return gatewayClient.innerClient.WalletSign(ctx, account, addr, msg, meta)
}
