package levelbolt

type Bucket struct {
	Name []byte
	tx   *Tx
}

func newBucket(name []byte, tx *Tx) *Bucket {
	return &Bucket{
		Name: name,
		tx:   tx,
	}
}
func (b *Bucket) Put(key, value []byte) error {
	return b.tx.Put(append(b.Name, key...), value)
}
func (b *Bucket) Delete(key []byte) error {
	return b.tx.Delete(append(b.Name, key...))
}
func (b *Bucket) Get(key []byte) []byte {
	return b.tx.Get(append(b.Name, key...))
}
func (b *Bucket) ForEach(cb func(k, v []byte) error) error {
	return b.tx.ForEach(b.Name, func(k, v []byte) error {
		return cb(k[len(b.Name):], v)
	})
}
func (b *Bucket) IsEmpty() bool {
	return b.tx.IsEmpty(b.Name)
}
