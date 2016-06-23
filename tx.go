package levelbolt

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type Tx struct {
	db      *leveldb.DB
	tx      *leveldb.Transaction
	buckets map[string]*Bucket
}

func newTx(db *leveldb.DB, readonly bool) *Tx {
	if !readonly {
		tx, err := db.OpenTransaction()
		if err != nil {
			panic(err)
		}
		return &Tx{db, tx, map[string]*Bucket{}}
	} else {
		return &Tx{db, nil, map[string]*Bucket{}}
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
		b = newBucket(name, t.db, t.tx)
		t.buckets[string(name)] = b
		return b
	}
}
