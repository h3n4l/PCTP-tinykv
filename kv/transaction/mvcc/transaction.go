package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	encodeKey := EncodeKey(key, ts)
	w := storage.Modify{
		Data: storage.Put{
			Key:   encodeKey,
			Value: write.ToBytes(),
			Cf:    "write",
		},
	}
	txn.writes = append(txn.writes, w)
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	it := txn.Reader.IterCF("lock")
	for it.Valid() {
		item := it.Item()
		if isSameKey(item.Key(), key) {
			value, vErr := item.Value()
			if vErr != nil {
				return nil, vErr
			}
			lock, lErr := ParseLock(value)
			if lErr != nil {
				return nil, lErr
			}
			return lock, nil
		}
		it.Next()
	}
	return nil, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	w := storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: lock.ToBytes(),
			Cf:    "lock",
		},
	}
	txn.writes = append(txn.writes, w)
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	w := storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  "lock",
		},
	}
	txn.writes = append(txn.writes, w)
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	it := txn.Reader.IterCF("write")
	for it.Valid() {
		item := it.Item()
		userKey := DecodeUserKey(item.Key())
		if isSameKey(userKey, key) {
			iv, iErr := item.Value()
			if iErr != nil {
				return nil, iErr
			}
			write, pErr := ParseWrite(iv)
			if pErr != nil {
				return nil, pErr
			}
			its := decodeTimestamp(item.Key())
			if write.Kind == WriteKindDelete && its == txn.StartTS {
				return nil, nil
			}
			if txn.StartTS < its {
				it.Next()
				continue
			}
			if write.StartTS < txn.StartTS {
				e := EncodeKey(key, write.StartTS)
				v, rErr := txn.Reader.GetCF("default", e)
				if rErr != nil {
					return nil, rErr
				}
				return v, nil
			}
		}
		it.Next()
	}
	return nil, nil
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	ts := txn.StartTS
	encodeKey := EncodeKey(key, ts)

	w := storage.Modify{
		Data: storage.Put{
			Key:   encodeKey,
			Value: value,
			Cf:    "default",
		},
	}
	txn.writes = append(txn.writes, w)
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	ts := txn.StartTS
	encodedKey := EncodeKey(key, ts)

	w := storage.Modify{
		Data: storage.Delete{
			Key: encodedKey,
			Cf:  "default",
		},
	}

	txn.writes = append(txn.writes, w)
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	//Your Code Here (4A).
	it := txn.Reader.IterCF("write")
	for it.Valid() {
		item := it.Item()
		userKey := DecodeUserKey(item.Key())
		if isSameKey(userKey, key) {
			value, vErr := item.Value()
			if vErr != nil {
				return nil, 0, vErr
			}
			w, wErr := ParseWrite(value)
			if wErr != nil {
				return nil, 0, wErr
			}
			if w.StartTS == txn.StartTS {
				cTs := decodeTimestamp(item.Key())
				return w, cTs, nil
			}
		}
		it.Next()
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	it := txn.Reader.IterCF("write")
	for it.Valid() {
		item := it.Item()
		userKey := DecodeUserKey(item.Key())
		if isSameKey(userKey, key) {
			value, vErr := item.Value()
			if vErr != nil {
				return nil, 0, vErr
			}
			w, wErr := ParseWrite(value)
			if wErr != nil {
				return nil, 0, wErr
			}
			cTs := decodeTimestamp(item.Key())
			return w, cTs, nil
		}
		it.Next()
	}
	return nil, 0, nil
}

func isSameKey(b1, b2 []byte) bool {
	if compare := bytes.Compare(b1, b2); compare == 0 {
		return true
	}
	return false
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
