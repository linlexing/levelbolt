/*用bolt的接口方法来改写leveldb，主要证据buckets的支持，以及事务的写法*/
package levelbolt

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type DB struct {
	db *leveldb.DB
}

func Open(filename string, op *opt.Options) (*DB, error) {
	db, err := leveldb.OpenFile(filename, op)
	if err != nil {
		return nil, err
	}
	rev := &DB{
		db: db,
	}
	return rev, nil
}
func (d *DB) Close() error {
	return d.db.Close()
}

//好像性能有问题，比不上Update
func (d *DB) Batch1(cb func(*Tx) error, wo *opt.WriteOptions) (err error) {
	tx := newBatchTx(d.db)
	defer func() {
		if err == nil {
			err = d.db.Write(tx.batch, wo)
		}
	}()
	err = cb(tx)
	return
}
func (d *DB) Begin() *Tx {
	return newTx(d.db, false)
}
func (d *DB) Update(cb func(*Tx) error) (err error) {
	tx := newTx(d.db, false)
	defer func() {
		if err != nil {
			tx.Discard()
		} else {
			err = tx.Commit()
		}
	}()
	err = cb(tx)
	return
}
func (d *DB) View(cb func(*Tx) error) error {
	tx := newTx(d.db, true)
	return cb(tx)
}
