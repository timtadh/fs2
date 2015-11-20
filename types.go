package fs2


type MultiMap interface {
	Keys() (ItemIterator, error)
	Values() (ItemIterator, error)
	Iterate() (Iterator, error)
	Backward() (Iterator, error)
	Find(key []byte) (Iterator, error)
	DoFind(key []byte, do func([]byte, []byte) error) error
	Range(from, to []byte) (Iterator, error)
	DoRange(from, to []byte, do func([]byte, []byte) error) error
	Has(key []byte) (bool, error)
	Count(key []byte) (int, error)
	Add(key []byte, value []byte) error
	Remove(key []byte, where func([]byte) bool) error
	Size() int
}

type Iterator func() ([]byte, []byte, error, Iterator)
type ItemIterator func() ([]byte, error, ItemIterator)

func Do(run func() (Iterator, error), do func(key []byte, value []byte) error) error {
	kvi, err := run()
	if err != nil {
		return err
	}
	var key []byte
	var value []byte
	for key, value, err, kvi = kvi(); kvi != nil; key, value, err, kvi = kvi() {
		e := do(key, value)
		if e != nil {
			return e
		}
	}
	return err
}

func DoItem(run func() (ItemIterator, error), do func([]byte) error) error {
	it, err := run()
	if err != nil {
		return err
	}
	var item []byte
	for item, err, it = it(); it != nil; item, err, it = it() {
		e := do(item)
		if e != nil {
			return e
		}
	}
	return err
}

