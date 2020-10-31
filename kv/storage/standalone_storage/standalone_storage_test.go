package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestStandAloneStorage(t *testing.T) {

	conf := config.NewTestConfig()
	conf.DBPath = `C:\Users\86155\badger`
	s := NewStandAloneStorage(conf)
	err := s.Start()
	defer s.Stop()
	assert.NoError(t, err)

	modifies := make([]storage.Modify, 0)
	modifies = append(modifies, storage.Modify{Data: storage.Put{
		Key:   []byte("a"),
		Value: []byte("ax"),
		Cf:    "Xa",
	}})
	modifies = append(modifies, storage.Modify{Data: storage.Put{
		Key:   []byte("b"),
		Value: []byte("bx"),
		Cf:    "Xa",
	}})
	modifies = append(modifies, storage.Modify{Data: storage.Put{
		Key:   []byte("c"),
		Value: []byte("cx"),
		Cf:    "Xa",
	}})
	modifies = append(modifies, storage.Modify{Data: storage.Put{
		Key:   []byte("d"),
		Value: []byte("dx"),
		Cf:    "Xa",
	}})
	err = s.Write(nil, modifies)
	assert.NoError(t, err)

	storageReader, err := s.Reader(nil)
	//defer storageReader.Close()
	assert.NoError(t, err)
	val1, err := storageReader.GetCF("Xa", []byte("d"))
	assert.NoError(t, err)
	assert.Equal(t, val1, []byte("dx"))
	val2, err := storageReader.GetCF("Xa", []byte("c"))
	assert.Equal(t, val2, []byte("cx"))
	val3, err := storageReader.GetCF("Xa", []byte("c"))
	assert.Equal(t, val3, []byte("cx"))

	dbIter := storageReader.IterCF("Xa").(*engine_util.BadgerIterator)
	defer storageReader.Close()
	defer dbIter.Close()
	log.Println(dbIter.Valid())
	for dbIter.Rewind();dbIter.Valid(); dbIter.Next() {
		val, _ := dbIter.Item().Value()
		log.Println(string(val))
	}
}
