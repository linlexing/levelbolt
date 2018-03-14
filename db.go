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

func (d *DB) Begin() *Tx {
	return newTx(d.db, false)
}
func (d *DB) Update(cb func(*Tx) error) (err error) {
	tx := newTx(d.db, false)
	finish := false
	defer func() {
		//如果没有设置，说明是中途跳出，发生了异常
		//这里不捕获异常是要保留现场
		if !finish {
			tx.Discard()
		}
	}()
	if err = cb(tx); err != nil {
		tx.Discard()
	} else {
		err = tx.Commit()
	}
	finish = true
	return
}

//UpdateNoTrans 不用事务更新
func (d *DB) UpdateNoTrans(cb func(*Tx) error) (err error) {
	tx := newTx(d.db, true)
	finish := false
	defer func() {
		//如果没有设置，说明是中途跳出，发生了异常
		//这里不捕获异常是要保留现场
		if !finish {
			tx.Discard()
		}
	}()
	if err = cb(tx); err != nil {
		tx.Discard()
	} else {
		err = tx.Commit()
	}
	finish = true
	return
}
func (d *DB) View(cb func(*Tx) error) error {
	tx := newTx(d.db, true)
	return cb(tx)
}
