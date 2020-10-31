package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	tinkvlog "github.com/pingcap-incubator/tinykv/log"
)

type StandAloneStorageReader struct {
	iterTxn *badger.Txn
	standAloneStorage *StandAloneStorage
}

func NewStandAloneStorageReader(storage *StandAloneStorage) (*StandAloneStorageReader, error) {
	if storage.kvDB == nil {
		tinkvlog.Error("db could be not nil")
		return nil, errors.New("db could be not nil")
	}
	return &StandAloneStorageReader{
		standAloneStorage:	storage,
	}, nil
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(sr.standAloneStorage.kvDB, cf, key)
	if err != nil {
		if err.Error() == "Key not found" {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return val, nil
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	if sr.iterTxn != nil {
		sr.iterTxn.Commit()
	}
	txn := sr.standAloneStorage.kvDB.NewTransaction(false)
	sr.iterTxn = txn
	return engine_util.NewCFIterator(cf, txn)
}

func (sr *StandAloneStorageReader) Close() {
	if sr.iterTxn != nil {
		sr.iterTxn.Commit()
	}
}
