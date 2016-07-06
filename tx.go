package levelbolt

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Tx struct {
	db      *leveldb.DB
	tx      *leveldb.Transaction
	batch   *leveldb.Batch
	buckets map[string]*Bucket
}

func newBatchTx(db *leveldb.DB) *Tx {

	return &Tx{db, nil, new(leveldb.Batch), map[string]*Bucket{}}
}
func newTx(db *leveldb.DB, readonly bool) *Tx {
	if !readonly {
		tx, err := db.OpenTransaction()
		if err != nil {
			panic(err)
		}
		return &Tx{db, tx, nil, map[string]*Bucket{}}
	} else {
		return &Tx{db, nil, nil, map[string]*Bucket{}}
	}
}
func (t *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	return t.Bucket(name), nil
}
func (t *Tx) CreateBucket(name []byte) *Bucket {
	return t.Bucket(name)
}
func (t *Tx) Bucket(name []byte) *Bucket {
	if b, ok := t.buckets[string(name)]; ok {
		return b
	} else {
		b = newBucket(name, t)
		t.buckets[string(name)] = b
		return b
	}
}
func (t *Tx) Get(key []byte) []byte {
	var rev []byte
	var err error
	if t.tx != nil {
		rev, err = t.tx.Get(key, nil)
	} else {
		rev, err = t.db.Get(key, nil)
	}

	if err == leveldb.ErrNotFound {
		return nil
	}
	if err != nil {
		panic(err)
	}
	return rev
}
func (t *Tx) Put(key, value []byte) error {
	if t.tx != nil {
		return t.tx.Put(key, value, nil)
	} else if t.batch != nil {
		t.batch.Put(key, value)
		return nil
	} else {
		return fmt.Errorf("view can't call put")
	}
}
func (t *Tx) Delete(key []byte) error {
	if t.tx != nil {
		return t.tx.Delete(key, nil)
	} else if t.batch != nil {
		t.batch.Delete(key)
		return nil
	} else {
		return fmt.Errorf("view can't call delete")
	}
}
func (t *Tx) ForEach(prex []byte, cb func(k, v []byte) error) error {
	iter := t.db.NewIterator(util.BytesPrefix(prex), nil)
	defer iter.Release()
	for iter.Next() {
		if err := cb(iter.Key(), iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}
func (t *Tx) IsEmpty(prex []byte) bool {
	iter := t.db.NewIterator(util.BytesPrefix(prex), nil)
	defer iter.Release()
	return !iter.First()
}
