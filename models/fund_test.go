package models

import (
	"os"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/venus-market/models/badger"
	"github.com/filecoin-project/venus-market/models/itf"
	"github.com/filecoin-project/venus-market/types"
	"github.com/stretchr/testify/assert"
)

func TestFund(t *testing.T) {
	t.Run("mysql", func(t *testing.T) {
		testFund(t, mysqlDB(t).FundRepo())
	})

	t.Run("badger", func(t *testing.T) {
		path := "./badger_fund_db"
		db := badgerDB(t, path)
		defer func() {
			assert.Nil(t, db.Close())
			assert.Nil(t, os.RemoveAll(path))

		}()
		testFund(t, itf.FundRepo(badger.NewFundStore(db)))
	})
}

func testFund(t *testing.T, fundRepo itf.FundRepo) {
	msgCid := randCid(t)
	state := &types.FundedAddressState{
		Addr:        randAddress(t),
		AmtReserved: abi.NewTokenAmount(100),
		MsgCid:      &msgCid,
	}

	state2 := &types.FundedAddressState{
		Addr:        randAddress(t),
		AmtReserved: abi.NewTokenAmount(10),
	}

	assert.Nil(t, fundRepo.SaveFundedAddressState(state))
	assert.Nil(t, fundRepo.SaveFundedAddressState(state2))

	res, err := fundRepo.GetFundedAddressState(state.Addr)
	assert.Nil(t, err)
	compareState(t, res, state)
	res2, err := fundRepo.GetFundedAddressState(state2.Addr)
	assert.Nil(t, err)
	compareState(t, res2, state2)

	res.AmtReserved = abi.NewTokenAmount(101)
	newCid := randCid(t)
	res.MsgCid = &newCid
	assert.Nil(t, fundRepo.SaveFundedAddressState(res))
	res3, err := fundRepo.GetFundedAddressState(res.Addr)
	assert.Nil(t, err)
	compareState(t, res, res3)

	list, err := fundRepo.ListFundedAddressState()
	assert.Nil(t, err)
	assert.GreaterOrEqual(t, len(list), 2)
}

func compareState(t *testing.T, actual, expected *types.FundedAddressState) {
	assert.Equal(t, expected.Addr, actual.Addr)
	assert.Equal(t, expected.AmtReserved, actual.AmtReserved)
	assert.Equal(t, expected.MsgCid, actual.MsgCid)
}
