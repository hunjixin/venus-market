package piecestorage

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/filecoin-project/venus-market/config"
	"golang.org/x/xerrors"
)

type Protocol string

type ProtocolParser func(cfg string) (interface{}, error)
type PieceStorageCtor func(cfg interface{}) (IPieceStorage, error)

type ProtocolResolver struct {
	Parser      ProtocolParser
	Constructor PieceStorageCtor
}

const (
	FS        Protocol = "fs"
	S3        Protocol = "s3"
	PreSignS3 Protocol = "presigns3"
)

var protocolRegistry map[Protocol]ProtocolResolver
var lk sync.Mutex

func init() {
	protocolRegistry = map[Protocol]ProtocolResolver{}

	RegisterPieceStorage(FS, ProtocolResolver{
		Parser: func(cfg string) (interface{}, error) {
			return config.FsPieceStorage{
				Enable: true,
				Path:   cfg,
			}, nil
		},
		Constructor: func(cfg interface{}) (IPieceStorage, error) {
			return newFsPieceStorage(cfg.(config.FsPieceStorage))
		},
	})

	RegisterPieceStorage(S3, ProtocolResolver{
		Parser: func(cfg string) (interface{}, error) {
			return ParserS3(cfg)
		},
		Constructor: func(cfg interface{}) (IPieceStorage, error) {
			return newS3PieceStorage(cfg.(config.S3PieceStorage))
		},
	})

	RegisterProtocolParser(PreSignS3, func(cfg string) (interface{}, error) {
		return config.PreSignS3PieceStorage{Enable: true}, nil
	})
}

func RegisterPieceStorage(protocol Protocol, resolver ProtocolResolver) {
	lk.Lock()
	defer lk.Unlock()
	protocolRegistry[protocol] = resolver
}

func RegisterProtocolParser(protocol Protocol, parser ProtocolParser) {
	lk.Lock()
	defer lk.Unlock()
	resolver, ok := protocolRegistry[protocol]
	if ok {
		resolver.Parser = parser
	}
	protocolRegistry[protocol] = ProtocolResolver{
		Parser: parser,
	}
}

func RegisterPieceStorageCtor(protocol Protocol, ctor PieceStorageCtor) {
	lk.Lock()
	defer lk.Unlock()
	resolver, ok := protocolRegistry[protocol]
	if ok {
		resolver.Constructor = ctor
	}
	protocolRegistry[protocol] = ProtocolResolver{
		Constructor: ctor,
	}
}

func GetPieceProtocolResolve(protocol Protocol) (ProtocolResolver, error) {
	lk.Lock()
	defer lk.Unlock()
	resolver, ok := protocolRegistry[protocol]
	if !ok {
		return ProtocolResolver{}, xerrors.Errorf("unable to find resolver for protocol %s", protocol)
	}
	return resolver, nil
}

func ParserProtocol(pro string, cfg interface{}) error {
	valCfg := reflect.ValueOf(cfg)
	if valCfg.Type().Kind() != reflect.Ptr {
		return xerrors.Errorf("recevie type not a pointer")
	}
	valCfg = valCfg.Elem()
	fIndex := strings.Index(pro, ":")
	if fIndex == -1 {
		return fmt.Errorf("parser piece storage %s", pro)
	}

	protocol := pro[:fIndex]
	dsn := pro[fIndex+1:]

	resolver, err := GetPieceProtocolResolve(Protocol(protocol))
	if err != nil {
		return err
	}
	fieldName, err := lookupMethod(valCfg.Type(), protocol)
	if err != nil {
		return err
	}

	storageCfg, err := resolver.Parser(dsn)
	if err != nil {
		return err
	}
	valCfg.FieldByName(fieldName).Set(reflect.ValueOf(storageCfg))
	return nil
}

func lookupMethod(val reflect.Type, name string) (string, error) {
	for i := 0; i < val.NumField(); i++ {
		if strings.EqualFold(val.Field(i).Name, name) {
			return val.Field(i).Name, nil
		}
	}
	return "", xerrors.Errorf("unable to find protocol config %s", name)
}
