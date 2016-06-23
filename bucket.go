package levelbolt

import (
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Bucket struct {
	Name []byte
	db   *leveldb.DB
	tx   *leveldb.Transaction
}

func newBucket(name []byte, db *leveldb.DB, tx *leveldb.Transaction) *Bucket {
	return &Bucket{
		Name: name,
		db:   db,
		tx:   tx,
	}
}
func (b *Bucket) Put(key, value []byte) error {
	if b.tx != nil {
		return b.tx.Put(append(b.Name, key...), value, nil)
	} else {
		return fmt.Errorf("view can't call put")
	}
}
func (b *Bucket) Delete(key []byte) error {
	if b.tx != nil {
		return b.tx.Delete(append(b.Name, key...), nil)
	} else {
		return fmt.Errorf("view can't call delete")
	}
}
func (b *Bucket) Get(key []byte) []byte {
	var rev []byte
	var err error
	if b.tx != nil {
		rev, err = b.tx.Get(append(b.Name, key...), nil)
	} else {
		rev, err = b.db.Get(append(b.Name, key...), nil)
	}

	if err == leveldb.ErrNotFound {
		return nil
	}
	if err != nil {
		panic(err)
	}
	return rev
}
func (b *Bucket) ForEach(cb func(k, v []byte) error) error {
	iter := b.db.NewIterator(util.BytesPrefix(b.Name), nil)
	defer iter.Release()
	for iter.Next() {
		if err := cb(iter.Key()[len(b.Name):], iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}
func (b *Bucket) IsEmpty() bool {
	iter := b.db.NewIterator(util.BytesPrefix(b.Name), nil)
	defer iter.Release()
	return !iter.First()
}
