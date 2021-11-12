package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines // storage engine
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath := conf.DBPath + "/kv"
	raftPath := conf.DBPath + "/raft"
	kvEngine := engine_util.CreateDB(kvPath, false)
	raftEngine := engine_util.CreateDB(raftPath, true)
	var engine *engine_util.Engines
	if conf.Raft {
		engine = engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)
	} else {
		engine = engine_util.NewEngines(kvEngine, nil, kvPath, raftPath)
	}
	return &StandAloneStorage{
		engine: engine,
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &StandAloneReader{
		txn: s.engine.Kv.NewTransaction(false),
	}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			if err := engine_util.PutCF(s.engine.Kv, modify.Cf(), modify.Key(), modify.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := engine_util.DeleteCF(s.engine.Kv, modify.Cf(), modify.Key()); err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}
func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
	r.txn.Discard()
}
