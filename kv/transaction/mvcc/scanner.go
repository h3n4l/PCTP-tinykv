package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	next []byte
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	return &Scanner{
		next: startKey,
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.next == nil {
		return nil, nil, nil
	}
	key := scan.next
	scan.iter.Seek(EncodeKey(key, scan.txn.StartTS))
	if !scan.iter.Valid() {
		scan.next = nil
		return nil, nil, nil
	}
	item := scan.iter.Item()
	userKey := DecodeUserKey(item.Key())
	currentTs := decodeTimestamp(item.Key())
	for scan.iter.Valid() && currentTs > scan.txn.StartTS {
		scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
		item = scan.iter.Item()
		currentTs = decodeTimestamp(item.Key())
		userKey = DecodeUserKey(item.Key())
	}
	if !scan.iter.Valid() {
		scan.next = nil
		return nil, nil, nil
	}
	for ; scan.iter.Valid(); scan.iter.Next() {
		nextUserKey := DecodeUserKey(scan.iter.Item().Key())
		if !bytes.Equal(nextUserKey, userKey) {
			scan.next = nextUserKey
			break
		}
	}
	if !scan.iter.Valid() {
		scan.next = nil
	}
	value, err := item.Value()
	if err != nil {
		return userKey, nil, err
	}
	write, err := ParseWrite(value)
	if err != nil {
		return userKey, nil, err
	}
	if write.Kind != WriteKindPut {
		return userKey, nil, nil
	}
	v, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	return userKey, v, err
}
