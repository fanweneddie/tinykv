package standalone_storage

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	// Import this library instead of "github.com/dgraph-io/badger"
	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	// We only need a database since it is single-node
	database *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	// Open a database at the given path
	db := engine_util.CreateDB(conf.DBPath, false)
	return &StandAloneStorage{
		database: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// Do sanity check and return error (if possible)
	if s.database == nil {
		return fmt.Errorf("Database cannot be nil")
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.database.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// Create a transaction from the database, because it can provide a consistent snapshot
	txn := s.database.NewTransaction(false)
	// err marks whether the transaction exists
	var err error = nil
	if txn == nil {
		err = fmt.Errorf("Transaction cannot be nil")
	}

	// Return an object of StandAloneStorageReader, which implements StorageReader
	return &StandAloneStorageReader{
		txn: txn,
	}, err
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if len(batch) > 0 {
		// We leverage a write batch to write key/value into the database
		writeBatch := new (engine_util.WriteBatch)
		for _, op := range batch {
			// Check the type of each write operation
			switch op.Data.(type) {
			case storage.Put:
				writeBatch.SetCF(op.Cf(), op.Key(), op.Value())
			case storage.Delete:
				writeBatch.DeleteCF(op.Cf(), op.Key())
			}
		}
		return writeBatch.WriteToDB(s.database)
	}
	return nil
}

// The implementation of StorageReader for StandAlone KV
type StandAloneStorageReader struct {
	// A transaction for a consistent snapshot
	txn *badger.Txn
}

// Implementation of StorageReader::GetCF
// Get the value of {cf, key} by leveraging transaction
func (reader StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	// For key not found, we don't regard it as error
	if val == nil {
		err = nil
	}
	return val, err
}

// Implementation of StorageReader::IterCF
// Return a CFIterator, which implements DBIterator
func (reader StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

// Implementation of StorageReade::Close
func (reader StandAloneStorageReader) Close() {
	reader.txn.Discard()
}