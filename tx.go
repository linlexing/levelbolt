package levelbolt

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Tx struct {
	db      *leveldb.DB
	tx      *leveldb.Transaction
	buckets map[string]*Bucket
}

func newTx(db *leveldb.DB, noTans bool) *Tx {
	if !noTans {
		tx, err := db.OpenTransaction()
		if err != nil {
			panic(err)
		}
		return &Tx{db, tx, map[string]*Bucket{}}
	}
	return &Tx{db, nil, map[string]*Bucket{}}

}
func (t *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	return t.Bucket(name), nil
}
func (t *Tx) CreateBucket(name []byte) *Bucket {
	return t.Bucket(name)
}
func (t *Tx) Has(key []byte) bool {
	if t.tx != nil {
		rev, err := t.tx.Has(key, nil)
		if err != nil {
			panic(err)
		}
		return rev
	} else {
		rev, err := t.db.Has(key, nil)
		if err != nil {
			panic(err)
		}
		return rev
	}

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
	} else {
		return t.db.Put(key, value, nil)
	}
}
func (t *Tx) Delete(key []byte) error {
	if t.tx != nil {
		return t.tx.Delete(key, nil)
	} else {
		return t.db.Delete(key, nil)
	}
}
func (t *Tx) Discard() {
	if t.tx != nil {
		t.tx.Discard()
	}
}
func (t *Tx) Commit() error {
	if t.tx != nil {
		return t.tx.Commit()
	}
	return nil
}

//ForEach the key and value must be copy,may change the next
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
