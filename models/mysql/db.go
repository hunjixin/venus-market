package mysql

import (
	"database/sql/driver"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/filecoin-project/venus-messager/models/mtypes"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/filecoin-project/venus-market/config"
	"github.com/filecoin-project/venus-market/models/repo"
)

type MysqlRepo struct {
	*gorm.DB
}

var _ repo.Repo = MysqlRepo{}

func (r MysqlRepo) GetDb() *gorm.DB {
	return r.DB
}

func (r MysqlRepo) FundRepo() repo.FundRepo {
	return NewFundedAddressStateRepo(r.GetDb())
}

func (r MysqlRepo) StorageDealRepo() repo.StorageDealRepo {
	return NewStorageDealRepo(r.GetDb())
}

func (r MysqlRepo) PaychMsgInfoRepo() repo.PaychMsgInfoRepo {
	return NewMsgInfoRepo(r.GetDb())
}

func (r MysqlRepo) PaychChannelInfoRepo() repo.PaychChannelInfoRepo {
	return NewChannelInfoRepo(r.GetDb())
}

func (r MysqlRepo) StorageAskRepo() repo.IStorageAskRepo {
	return NewStorageAskRepo(r.GetDb())
}

func (r MysqlRepo) RetrievalAskRepo() repo.IRetrievalAskRepo {
	return NewRetrievalAskRepo(r.GetDb())
}

func (r MysqlRepo) CidInfoRepo() repo.ICidInfoRepo {
	return NewMysqlCidInfoRepo(r.GetDb())
}

func (r MysqlRepo) RetrievalDealRepo() repo.IRetrievalDealRepo {
	return NewRetrievalDealRepo(r.GetDb())
}

func (r MysqlRepo) Close() error {
	db, err := r.DB.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

func (r MysqlRepo) Migrate() error {
	err := r.GetDb().AutoMigrate(cidInfo{})
	if err != nil {
		return err
	}

	err = r.GetDb().AutoMigrate(fundedAddressState{})
	if err != nil {
		return err
	}

	err = r.GetDb().AutoMigrate(channelInfo{})
	if err != nil {
		return err
	}

	err = r.GetDb().AutoMigrate(retrievalAsk{})
	if err != nil {
		return err
	}

	err = r.GetDb().AutoMigrate(retrievalDeal{})
	if err != nil {
		return err
	}

	err = r.GetDb().AutoMigrate(storageDeal{})
	if err != nil {
		return err
	}

	err = r.GetDb().AutoMigrate(storageAsk{})
	if err != nil {
		return err
	}
	return nil
}

func (r MysqlRepo) Transaction(cb func(txRepo repo.TxRepo) error) error {
	return r.GetDb().Transaction(func(tx *gorm.DB) error {
		return cb(txRepo{tx})
	})
}

type txRepo struct {
	*gorm.DB
}

func (r txRepo) StorageDealRepo() repo.StorageDealRepo {
	return NewStorageDealRepo(r.DB)
}

func InitMysql(cfg *config.Mysql) (repo.Repo, error) {
	gorm.ErrRecordNotFound = repo.ErrNotFound
	db, err := gorm.Open(mysql.Open(cfg.ConnectionString))
	if err != nil {
		return nil, xerrors.Errorf("[db connection failed] Database name: %s %w", cfg.ConnectionString, err)
	}

	db.Set("gorm:table_options", "CHARSET=utf8mb4")
	if cfg.Debug {
		db = db.Debug()
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxOpenConns(cfg.MaxOpenConn)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConn)
	d, err := time.ParseDuration(cfg.ConnMaxLifeTime)
	if err != nil {
		return nil, err
	}
	sqlDB.SetConnMaxLifetime(d)

	r := &MysqlRepo{DB: db}

	return r, r.AutoMigrate(retrievalAsk{}, cidInfo{}, storageAsk{}, fundedAddressState{}, storageDeal{}, channelInfo{}, msgInfo{})
}

type DBCid cid.Cid

var UndefDBCid = DBCid{}

func (c *DBCid) Scan(value interface{}) error {
	val, ok := value.([]byte)
	if !ok {
		return xerrors.New("cid should be a `[]byte`")
	}
	if len(val) == 0 {
		*c = UndefDBCid
		return nil
	}
	cid, err := cid.Decode(string(val))
	if err != nil {
		return err
	}
	*c = DBCid(cid)

	return nil
}

func (c DBCid) Value() (driver.Value, error) {
	return c.String(), nil
}

func (c DBCid) String() string {
	if c == UndefDBCid {
		return ""
	}
	return cid.Cid(c).String()
}

func (c DBCid) cid() cid.Cid {
	return cid.Cid(c)
}

func (c DBCid) cidPtr() *cid.Cid {
	if c == UndefDBCid {
		return nil
	}
	cid := cid.Cid(c)
	return &cid
}

func convertBigInt(v big.Int) mtypes.Int {
	if v.Nil() {
		return mtypes.NewInt(0)
	}
	return mtypes.NewFromGo(v.Int)
}

func decodePeerId(str string) (peer.ID, error) {
	return peer.Decode(str)
}

type DBAddress address.Address

var UndefDBAddress = DBAddress{}

func (a *DBAddress) Scan(value interface{}) error {
	val, ok := value.([]byte)
	if !ok {
		return xerrors.New("address should be a `[]byte`")
	}
	if len(val) == 0 {
		*a = UndefDBAddress
		return nil
	}
	addr, err := address.NewFromString(address.MainnetPrefix + string(val))
	if err != nil {
		return err
	}
	*a = DBAddress(addr)

	return nil
}

func (a DBAddress) Value() (driver.Value, error) {
	return a.String(), nil
}

func (a DBAddress) String() string {
	if a == UndefDBAddress {
		return ""
	}
	// Remove the prefix identifying the network type，eg. change `f01000` to `01000`
	return address.Address(a).String()[1:]
}

func (a DBAddress) addr() address.Address {
	return address.Address(a)
}

func (a DBAddress) addrPtr() *address.Address {
	if a == UndefDBAddress {
		return nil
	}
	addr := address.Address(a)
	return &addr
}

type TimeStampOrm struct {
	CreatedAt uint64 `gorm:"type:bigint unsigned"`
	UpdatedAt uint64 `gorm:"type:bigint unsigned"`
}
