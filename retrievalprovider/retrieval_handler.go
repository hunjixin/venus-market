package retrievalprovider

import (
	"context"
	"errors"
	"github.com/filecoin-project/venus-market/types"

	rm "github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-statemachine"
	"github.com/filecoin-project/venus-market/models/repo"
)

type IRetrievalHandler interface {
	UnsealData(ctx context.Context, deal *types.ProviderDealState) error
	CancelDeal(ctx context.Context, deal *types.ProviderDealState) error
	CleanupDeal(ctx context.Context, deal *types.ProviderDealState) error
	Error(ctx context.Context, deal *types.ProviderDealState, err error) error
}

var _ IRetrievalHandler = (*RetrievalDealHandler)(nil)

type RetrievalDealHandler struct {
	env                ProviderDealEnvironment
	retrievalDealStore repo.IRetrievalDealRepo
}

func NewRetrievalDealHandler(env ProviderDealEnvironment, retrievalDealStore repo.IRetrievalDealRepo) IRetrievalHandler {
	return &RetrievalDealHandler{env: env, retrievalDealStore: retrievalDealStore}
}

func (p *RetrievalDealHandler) UnsealData(ctx context.Context, deal *types.ProviderDealState) error {
	deal.Status = rm.DealStatusUnsealing
	err := p.retrievalDealStore.SaveDeal(deal)
	if err != nil {
		return err
	}

	if err := p.env.PrepareBlockstore(ctx, deal.ID, *deal.PieceCID); err != nil {
		return p.CancelDeal(ctx, deal)
	}
	log.Debugf("blockstore prepared successfully, firing unseal complete for deal %d", deal.ID)
	deal.Status = rm.DealStatusUnsealed
	err = p.retrievalDealStore.SaveDeal(deal)
	if err != nil {
		return err
	}

	log.Debugf("unpausing data transfer for deal %d", deal.ID)

	if deal.ChannelID != nil {
		log.Debugf("resuming data transfer for deal %d", deal.ID)
		err = p.env.ResumeDataTransfer(ctx, *deal.ChannelID)
		if err != nil {
			deal.Status = rm.DealStatusErrored
		}
	}
	return p.retrievalDealStore.SaveDeal(deal)
}

func (p *RetrievalDealHandler) CancelDeal(ctx context.Context, deal *types.ProviderDealState) error {
	// Read next response (or fail)
	err := p.env.DeleteStore(deal.ID)
	if err != nil {
		return p.Error(ctx, deal, nil)
	}
	if deal.ChannelID != nil {
		err = p.env.CloseDataTransfer(ctx, *deal.ChannelID)
		if err != nil && !errors.Is(err, statemachine.ErrTerminated) {
			return p.Error(ctx, deal, nil)
		}
	}
	deal.Status = rm.DealStatusCancelled
	return p.retrievalDealStore.SaveDeal(deal)
}

// CleanupDeal runs to do memory cleanup for an in progress deal
func (p *RetrievalDealHandler) CleanupDeal(ctx context.Context, deal *types.ProviderDealState) error {
	err := p.env.DeleteStore(deal.ID)
	if err != nil {
		return p.Error(ctx, deal, nil)
	}
	deal.Status = rm.DealStatusCompleted
	return p.retrievalDealStore.SaveDeal(deal)
}

func (p *RetrievalDealHandler) Error(ctx context.Context, deal *types.ProviderDealState, err error) error {
	deal.Status = rm.DealStatusErrored
	if err != nil {
		deal.Message = err.Error()
	}
	return p.retrievalDealStore.SaveDeal(deal)
}
