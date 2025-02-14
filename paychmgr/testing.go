package paychmgr

import (
	"github.com/filecoin-project/venus-market/models/badger"
	"github.com/filecoin-project/venus-market/models/repo"
	ds "github.com/ipfs/go-datastore"
	ds_sync "github.com/ipfs/go-datastore/sync"
)

func newRepo() repo.Repo {
	params := badger.BadgerDSParams{
		PaychDS: ds_sync.MutexWrap(ds.NewMapDatastore()),
	}
	return badger.NewBadgerRepo(params)
}
