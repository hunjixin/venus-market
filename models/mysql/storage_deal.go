package mysql

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/venus-market/models/repo"

	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/venus-market/types"

	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/filestore"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	mtypes "github.com/filecoin-project/venus-messager/types"
	"github.com/ipfs/go-cid"
	typegen "github.com/whyrusleeping/cbor-gen"
	"gorm.io/gorm"
)

const storageDealTableName = "storage_deals"

type storageDeal struct {
	ClientDealProposal `gorm:"embedded;embeddedPrefix:cdp_"`

	ProposalCid DBCid  `gorm:"column:proposal_cid;type:varchar(256);primary_key"`
	AddFundsCid DBCid  `gorm:"column:add_funds_cid;type:varchar(256);"`
	PublishCid  DBCid  `gorm:"column:publish_cid;type:varchar(256);"`
	Miner       string `gorm:"column:miner_peer;type:varchar(128);"`
	Client      string `gorm:"column:client_peer;type:varchar(128);"`
	State       uint64 `gorm:"column:state;type:bigint unsigned;index"`

	PayloadSize           int64      `gorm:"column:payload_size;type:bigint;"`
	PiecePath             string     `gorm:"column:piece_path;type:varchar(256);"`
	MetadataPath          string     `gorm:"column:metadata_path;type:varchar(256);"`
	SlashEpoch            int64      `gorm:"column:slash_epoch;type:bigint;"`
	FastRetrieval         bool       `gorm:"column:fast_retrieval;"`
	Message               string     `gorm:"column:message;type:varchar(512);"`
	FundsReserved         mtypes.Int `gorm:"column:funds_reserved;type:varchar(256);"`
	Ref                   DataRef    `gorm:"embedded;embeddedPrefix:ref_"`
	AvailableForRetrieval bool       `gorm:"column:available_for_retrieval;"`

	DealID       uint64 `gorm:"column:deal_id;type:bigint unsigned;index"`
	CreationTime int64  `gorm:"column:creation_time;type:bigint;"`

	TransferChannelId ChannelID `gorm:"embedded;embeddedPrefix:tci_"`
	SectorNumber      uint64    `gorm:"column:sector_number;type:bigint unsigned;"`

	InboundCAR string `gorm:"column:addr;type:varchar(256);"`

	Offset      uint64 `gorm:"column:offset;type:bigint"`
	Length      uint64 `gorm:"column:length;type:bigint"`
	PieceStatus string `gorm:"column:piece_status;type:varchar(128);index"`

	TimeStampOrm
}

type ClientDealProposal struct {
	PieceCID     DBCid     `gorm:"column:piece_cid;type:varchar(256);index"`
	PieceSize    uint64    `gorm:"column:piece_size;type:bigint unsigned;"`
	VerifiedDeal bool      `gorm:"column:verified_deal;"`
	Client       DBAddress `gorm:"column:client;type:varchar(256);"`
	Provider     DBAddress `gorm:"column:provider;type:varchar(256);index"`

	// Label is an arbitrary client chosen label to apply to the deal
	Label string `gorm:"column:label;type:varchar(256);"`

	// Nominal start epoch. Deal payment is linear between StartEpoch and EndEpoch,
	// with total amount StoragePricePerEpoch * (EndEpoch - StartEpoch).
	// Storage deal must appear in a sealed (proven) sector no later than StartEpoch,
	// otherwise it is invalid.
	StartEpoch           int64      `gorm:"column:start_epoch;type:bigint;"`
	EndEpoch             int64      `gorm:"column:end_epoch;type:bigint;"`
	StoragePricePerEpoch mtypes.Int `gorm:"column:storage_price_per_epoch;type:varchar(256);"`

	ProviderCollateral mtypes.Int `gorm:"column:provider_collateral;type:varchar(256);"`
	ClientCollateral   mtypes.Int `gorm:"column:client_collateral;type:varchar(256);"`

	ClientSignature Signature `gorm:"column:client_signature;type:blob;"`
}

type Signature acrypto.Signature

func (s *Signature) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("value must be []byte")
	}
	return json.Unmarshal(b, s)
}

func (s Signature) Value() (driver.Value, error) {
	return json.Marshal(s)
}

type ChannelID struct {
	Initiator string `gorm:"column:initiator;type:varchar(256);"`
	Responder string `gorm:"column:responder;type:varchar(256);"`
	ID        uint64 `gorm:"column:channel_id;type:bigint unsigned;"`
}

type DataRef struct {
	TransferType string `gorm:"column:transfer_type;type:varchar(128);"`
	Root         DBCid  `gorm:"column:root;type:varchar(256);"`

	PieceCid     DBCid                 `gorm:"column:piece_cid;type:varchar(256);"`
	PieceSize    abi.UnpaddedPieceSize `gorm:"column:piece_size;type:bigint unsigned;"`
	RawBlockSize uint64                `gorm:"column:raw_block_size;type:bigint unsigned;"`
}

func (m *storageDeal) TableName() string {
	return storageDealTableName
}

func fromStorageDeal(src *types.MinerDeal) *storageDeal {
	md := &storageDeal{
		ClientDealProposal: ClientDealProposal{
			PieceCID:             DBCid(src.ClientDealProposal.Proposal.PieceCID),
			PieceSize:            uint64(src.ClientDealProposal.Proposal.PieceSize),
			VerifiedDeal:         src.ClientDealProposal.Proposal.VerifiedDeal,
			Client:               DBAddress(src.ClientDealProposal.Proposal.Client),
			Provider:             DBAddress(src.ClientDealProposal.Proposal.Provider),
			Label:                src.ClientDealProposal.Proposal.Label,
			StartEpoch:           int64(src.ClientDealProposal.Proposal.StartEpoch),
			EndEpoch:             int64(src.ClientDealProposal.Proposal.EndEpoch),
			StoragePricePerEpoch: convertBigInt(src.ClientDealProposal.Proposal.StoragePricePerEpoch),
			ProviderCollateral:   convertBigInt(src.ClientDealProposal.Proposal.ProviderCollateral),
			ClientCollateral:     convertBigInt(src.ClientDealProposal.Proposal.ClientCollateral),
			ClientSignature: Signature{
				Type: src.ClientSignature.Type,
				Data: src.ClientSignature.Data,
			},
		},
		ProposalCid:           DBCid(src.ProposalCid),
		Miner:                 src.Miner.Pretty(),
		Client:                src.Client.Pretty(),
		State:                 src.State,
		PayloadSize:           int64(src.PayloadSize),
		PiecePath:             string(src.PiecePath),
		MetadataPath:          string(src.MetadataPath),
		SlashEpoch:            int64(src.SlashEpoch),
		FastRetrieval:         src.FastRetrieval,
		Message:               src.Message,
		FundsReserved:         convertBigInt(src.FundsReserved),
		AvailableForRetrieval: src.AvailableForRetrieval,
		DealID:                uint64(src.DealID),
		CreationTime:          src.CreationTime.Time().UnixNano(),
		SectorNumber:          uint64(src.SectorNumber),
		InboundCAR:            src.InboundCAR,

		Offset:      uint64(src.Offset),
		Length:      uint64(src.Proposal.PieceSize),
		PieceStatus: src.PieceStatus,
	}

	if src.AddFundsCid == nil {
		md.AddFundsCid = UndefDBCid
	} else {
		md.AddFundsCid = DBCid(*src.AddFundsCid)
	}
	if src.PublishCid == nil {
		md.PublishCid = UndefDBCid
	} else {
		md.PublishCid = DBCid(*src.PublishCid)
	}

	if src.Ref != nil {
		md.Ref = DataRef{
			TransferType: src.Ref.TransferType,
			Root:         DBCid(src.Ref.Root),
			PieceSize:    src.Ref.PieceSize,
			RawBlockSize: src.Ref.RawBlockSize,
		}

		if src.Ref.PieceCid == nil {
			md.Ref.PieceCid = UndefDBCid
		} else {
			md.Ref.PieceCid = DBCid(*src.Ref.PieceCid)
		}
	}
	if src.TransferChannelId != nil {
		md.TransferChannelId = ChannelID{
			Initiator: src.TransferChannelId.Initiator.String(),
			Responder: src.TransferChannelId.Responder.String(),
			ID:        uint64(src.TransferChannelId.ID),
		}
	}

	return md
}

func toStorageDeal(src *storageDeal) (*types.MinerDeal, error) {
	md := &types.MinerDeal{
		ClientDealProposal: market.ClientDealProposal{
			Proposal: market.DealProposal{
				PieceCID:             src.PieceCID.cid(),
				PieceSize:            abi.PaddedPieceSize(src.PieceSize),
				VerifiedDeal:         src.VerifiedDeal,
				Client:               src.ClientDealProposal.Client.addr(),
				Provider:             src.ClientDealProposal.Provider.addr(),
				Label:                src.Label,
				StartEpoch:           abi.ChainEpoch(src.StartEpoch),
				EndEpoch:             abi.ChainEpoch(src.EndEpoch),
				StoragePricePerEpoch: abi.TokenAmount{Int: src.StoragePricePerEpoch.Int},
				ProviderCollateral:   abi.TokenAmount{Int: src.ProviderCollateral.Int},
				ClientCollateral:     abi.TokenAmount{Int: src.ClientCollateral.Int},
			},
			ClientSignature: acrypto.Signature{
				Type: src.ClientSignature.Type,
				Data: src.ClientSignature.Data,
			},
		},
		ProposalCid:   src.ProposalCid.cid(),
		AddFundsCid:   src.AddFundsCid.cidPtr(),
		PublishCid:    src.PublishCid.cidPtr(),
		State:         src.State,
		PayloadSize:   abi.UnpaddedPieceSize(src.PayloadSize),
		PiecePath:     filestore.Path(src.PiecePath),
		MetadataPath:  filestore.Path(src.MetadataPath),
		PieceStatus:   src.PieceStatus,
		SlashEpoch:    abi.ChainEpoch(src.SlashEpoch),
		FastRetrieval: src.FastRetrieval,
		Message:       src.Message,
		FundsReserved: abi.TokenAmount{Int: src.FundsReserved.Int},
		Ref: &storagemarket.DataRef{
			TransferType: src.Ref.TransferType,
			Root:         src.Ref.Root.cid(),
			PieceCid:     src.Ref.PieceCid.cidPtr(),
			PieceSize:    src.Ref.PieceSize,
			RawBlockSize: src.Ref.RawBlockSize,
		},
		AvailableForRetrieval: src.AvailableForRetrieval,
		DealID:                abi.DealID(src.DealID),
		CreationTime:          typegen.CborTime(time.Unix(0, src.CreationTime).UTC()),
		SectorNumber:          abi.SectorNumber(src.SectorNumber),
		InboundCAR:            src.InboundCAR,
		Offset:                abi.PaddedPieceSize(src.Offset),
	}
	var err error

	if len(src.TransferChannelId.Initiator) > 0 {
		md.TransferChannelId = &datatransfer.ChannelID{}
		md.TransferChannelId.ID = datatransfer.TransferID(src.TransferChannelId.ID)
		md.TransferChannelId.Initiator, err = decodePeerId(src.TransferChannelId.Initiator)
		if err != nil {
			return nil, err
		}
		md.TransferChannelId.Responder, err = decodePeerId(src.TransferChannelId.Responder)
		if err != nil {
			return nil, err
		}
	}

	md.Miner, err = decodePeerId(src.Miner)
	if err != nil {
		return nil, err
	}
	md.Client, err = decodePeerId(src.Client)
	if err != nil {
		return nil, err
	}

	return md, nil
}

type storageDealRepo struct {
	*gorm.DB
}

var _ repo.StorageDealRepo = (*storageDealRepo)(nil)

func NewStorageDealRepo(db *gorm.DB) *storageDealRepo {
	return &storageDealRepo{db}
}

func (m *storageDealRepo) SaveDeal(StorageDeal *types.MinerDeal) error {
	dbDeal := fromStorageDeal(StorageDeal)
	dbDeal.UpdatedAt = uint64(time.Now().Unix())
	return m.DB.Save(dbDeal).Error
}

func (m *storageDealRepo) GetDeal(proposalCid cid.Cid) (*types.MinerDeal, error) {
	var md storageDeal
	err := m.DB.Take(&md, "proposal_cid = ?", DBCid(proposalCid).String()).Error
	if err != nil {
		return nil, err
	}

	return toStorageDeal(&md)
}

func (dsr *storageDealRepo) GetDeals(miner address.Address, pageIndex, pageSize int) ([]*types.MinerDeal, error) {
	var md []storageDeal

	err := dsr.DB.Table((&storageDeal{}).TableName()).
		Find(&md, "cdp_provider = ?", DBAddress(miner).String()).
		Offset(pageIndex * pageSize).Limit(pageSize).Error

	if err != nil {
		return nil, err
	}

	var deals = make([]*types.MinerDeal, len(md))

	for idx, deal := range md {
		if deals[idx], err = toStorageDeal(&deal); err != nil {
			return nil, xerrors.Errorf("convert StorageDeal(%s) to a types.MinerDeal failed:%w",
				deal.ProposalCid, err)
		}
	}

	return deals, nil
}

func (dsr *storageDealRepo) GetDealsByPieceCidAndStatus(piececid cid.Cid, statues ...storagemarket.StorageDealStatus) ([]*types.MinerDeal, error) {
	var md []storageDeal

	err := dsr.DB.Table((&storageDeal{}).TableName()).
		Find(&md, "cdp_piece_cid = ? AND state in ?", DBCid(piececid).String(), statues).Error

	if err != nil {
		return nil, err
	}

	var deals = make([]*types.MinerDeal, len(md))

	for idx, deal := range md {
		if deals[idx], err = toStorageDeal(&deal); err != nil {
			return nil, xerrors.Errorf("convert StorageDeal(%s) to a types.MinerDeal failed:%w",
				deal.ProposalCid, err)
		}
	}

	return deals, nil
}

func (dsr *storageDealRepo) GetDealByAddrAndStatus(addr address.Address, status ...storagemarket.StorageDealStatus) ([]*types.MinerDeal, error) {
	var md []storageDeal

	err := dsr.DB.Table((&storageDeal{}).TableName()).Find(&md, "cdp_provider = ? AND state in ?", DBAddress(addr).String(), status).Error
	if err != nil {
		return nil, err
	}

	var deals = make([]*types.MinerDeal, len(md))

	for idx, deal := range md {
		if deals[idx], err = toStorageDeal(&deal); err != nil {
			return nil, xerrors.Errorf("convert StorageDeal(%s) to a types.MinerDeal failed:%w",
				deal.ProposalCid, err)
		}
	}

	return deals, nil
}

func (dsr *storageDealRepo) UpdateDealStatus(proposalCid cid.Cid, status storagemarket.StorageDealStatus) error {
	return dsr.DB.Model(storageDeal{}).Where("proposal_cid = ?", DBCid(proposalCid).String()).
		UpdateColumns(map[string]interface{}{"state": status, "updated_at": time.Now().Unix()}).Error
}

func (m *storageDealRepo) ListDealByAddr(miner address.Address) ([]*types.MinerDeal, error) {
	var storageDeals []*storageDeal
	if err := m.Table(storageDealTableName).Find(storageDeals, "cdp_provider = ?", DBAddress(miner).String()).Error; err != nil {
		return nil, err
	}
	return fromDbDeals(storageDeals)
}

func (m *storageDealRepo) ListDeal() ([]*types.MinerDeal, error) {
	var storageDeals []*storageDeal
	if err := m.Table(storageDealTableName).Find(&storageDeals).Error; err != nil {
		return nil, err
	}
	return fromDbDeals(storageDeals)
}

func (m *storageDealRepo) GetPieceInfo(pieceCID cid.Cid) (*piecestore.PieceInfo, error) {
	var storageDeals []*storageDeal
	if err := m.Table(storageDealTableName).Find(&storageDeals, "cdp_piece_cid = ?", DBCid(pieceCID).String()).Error; err != nil {
		return nil, err
	}

	var pieceInfo = piecestore.PieceInfo{
		PieceCID: pieceCID,
		Deals:    nil,
	}

	for _, dbDeal := range storageDeals {
		deal, err := toStorageDeal(dbDeal)
		if err != nil {
			return nil, err
		}
		pieceInfo.Deals = append(pieceInfo.Deals, piecestore.DealInfo{
			DealID:   deal.DealID,
			SectorID: deal.SectorNumber,
			Offset:   deal.Offset,
			Length:   deal.Proposal.PieceSize},
		)
	}
	return &pieceInfo, nil
}

func (m *storageDealRepo) ListPieceInfoKeys() ([]cid.Cid, error) {
	var cidsStr []string
	var err error

	if err := m.DB.Table((&storageDeal{}).TableName()).Select("cdp_piece_cid").Scan(&cidsStr).Error; err != nil {
		return nil, err
	}

	cids := make([]cid.Cid, len(cidsStr))
	for idx, s := range cidsStr {
		cids[idx], err = cid.Decode(s)
		if err != nil {
			return nil, err
		}
	}

	return cids, nil
}

func (m *storageDealRepo) GetDealByDealID(mAddr address.Address, dealID abi.DealID) (*types.MinerDeal, error) {
	var dbDeal *storageDeal
	if err := m.DB.Table(storageDealTableName).Take(&dbDeal, "cdp_provider = ? and deal_id = ?", DBAddress(mAddr).String(), dealID).Error; err != nil {
		return nil, err
	}
	return toStorageDeal(dbDeal)
}

func (m *storageDealRepo) GetDealsByPieceStatus(mAddr address.Address, pieceStatus string) ([]*types.MinerDeal, error) {
	var dbDeals []*storageDeal
	if err := m.DB.Table(storageDealTableName).Find(&dbDeals, "cdp_provider = ? and piece_status = ?", DBAddress(mAddr).String(), pieceStatus).Error; err != nil {
		return nil, err
	}

	return fromDbDeals(dbDeals)
}

func (m *storageDealRepo) GetPieceSize(pieceCID cid.Cid) (abi.UnpaddedPieceSize, abi.PaddedPieceSize, error) {
	var deal *storageDeal
	if err := m.DB.Table(storageDealTableName).Take(&deal, "cdp_piece_cid = ? ", DBCid(pieceCID).String()).Error; err != nil {
		return 0, 0, err
	}

	return abi.UnpaddedPieceSize(deal.PayloadSize), abi.PaddedPieceSize(deal.PieceSize), nil
}

func fromDbDeals(dbDeals []*storageDeal) ([]*types.MinerDeal, error) {
	results := make([]*types.MinerDeal, len(dbDeals))
	for index, dbDeal := range dbDeals {
		deal, err := toStorageDeal(dbDeal)
		if err != nil {
			return nil, err
		}
		results[index] = deal
	}
	return results, nil
}
