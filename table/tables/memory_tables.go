// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"reflect"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

var store kv.Storage

// MemoryTable implements table.Table interface.
type MemoryTable struct {
	ID      int64
	Name    model.CIStr
	Columns []*column.Col

	recordPrefix kv.Key
	alloc        autoid.Allocator
	meta         *model.TableInfo

	store kv.Storage
}

// MemoryTableFromMeta creates a Table instance from model.TableInfo.
func MemoryTableFromMeta(alloc autoid.Allocator, tblInfo *model.TableInfo) (table.Table, error) {
	columns := make([]*column.Col, 0, len(tblInfo.Columns))
	for _, colInfo := range tblInfo.Columns {
		col := &column.Col{ColumnInfo: *colInfo}
		columns = append(columns, col)
	}
	t := newMemoryTable(tblInfo.ID, tblInfo.Name.O, columns, alloc)
	t.meta = tblInfo
	return t, nil
}

// newMemoryTable constructs a MemoryTable instance.
func newMemoryTable(tableID int64, tableName string, cols []*column.Col, alloc autoid.Allocator) *MemoryTable {
	name := model.NewCIStr(tableName)
	t := &MemoryTable{
		ID:           tableID,
		Name:         name,
		alloc:        alloc,
		Columns:      cols,
		store:        store,
		recordPrefix: genTableRecordPrefix(tableID),
	}
	return t
}

func (t *MemoryTable) GetTxn(ctx context.Context, forceNew bool) (kv.Transaction, error) {
	return t.store.Begin()
}

// TableID implements table.Table TableID interface.
func (t *MemoryTable) TableID() int64 {
	return t.ID
}

// Indices implements table.Table Indices interface.
func (t *MemoryTable) Indices() []*column.IndexedCol {
	return nil
}

// AddIndex implements table.Table AddIndex interface.
func (t *MemoryTable) AddIndex(idxCol *column.IndexedCol) {
	return
}

// TableName implements table.Table TableName interface.
func (t *MemoryTable) TableName() model.CIStr {
	return t.Name
}

// Meta implements table.Table Meta interface.
func (t *MemoryTable) Meta() *model.TableInfo {
	return t.meta
}

// Cols implements table.Table Cols interface.
func (t *MemoryTable) Cols() []*column.Col {
	return t.Columns
}

func (t *MemoryTable) unflatten(rec interface{}, col *column.Col) (interface{}, error) {
	if rec == nil {
		return nil, nil
	}
	switch col.Tp {
	case mysql.TypeFloat:
		return float32(rec.(float64)), nil
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeDouble, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob,
		mysql.TypeVarchar, mysql.TypeString:
		return rec, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t mysql.Time
		t.Type = col.Tp
		t.Fsp = col.Decimal
		err := t.Unmarshal(rec.([]byte))
		if err != nil {
			return nil, errors.Trace(err)
		}
		return t, nil
	case mysql.TypeDuration:
		return mysql.Duration{Duration: time.Duration(rec.(int64)), Fsp: col.Decimal}, nil
	case mysql.TypeNewDecimal, mysql.TypeDecimal:
		return mysql.ParseDecimal(string(rec.([]byte)))
	case mysql.TypeEnum:
		return mysql.ParseEnumValue(col.Elems, rec.(uint64))
	case mysql.TypeSet:
		return mysql.ParseSetValue(col.Elems, rec.(uint64))
	case mysql.TypeBit:
		return mysql.Bit{Value: rec.(uint64), Width: col.Flen}, nil
	}
	log.Error(col.Tp, rec, reflect.TypeOf(rec))
	return nil, nil
}

func (t *MemoryTable) flatten(data interface{}) (interface{}, error) {
	switch x := data.(type) {
	case mysql.Time:
		// for mysql datetime, timestamp and date type
		return x.Marshal()
	case mysql.Duration:
		// for mysql time type
		return int64(x.Duration), nil
	case mysql.Decimal:
		return x.String(), nil
	case mysql.Enum:
		return x.Value, nil
	case mysql.Set:
		return x.Value, nil
	case mysql.Bit:
		return x.Value, nil
	default:
		return data, nil
	}
}

// RecordPrefix implements table.Table RecordPrefix interface.
func (t *MemoryTable) RecordPrefix() kv.Key {
	return t.recordPrefix
}

// IndexPrefix implements table.Table IndexPrefix interface.
func (t *MemoryTable) IndexPrefix() kv.Key {
	return nil
}

// RecordKey implements table.Table RecordKey interface.
func (t *MemoryTable) RecordKey(h int64, col *column.Col) kv.Key {
	colID := int64(0)
	if col != nil {
		colID = col.ID
	}
	return encodeRecordKey(t.recordPrefix, h, colID)
}

// FirstKey implements table.Table FirstKey interface.
func (t *MemoryTable) FirstKey() kv.Key {
	return t.RecordKey(0, nil)
}

// FindIndexByColName implements table.Table FindIndexByColName interface.
func (t *MemoryTable) FindIndexByColName(name string) *column.IndexedCol {
	return nil
}

// Truncate implements table.Table Truncate interface.
func (t *MemoryTable) Truncate(rm kv.RetrieverMutator) error {
	txn, err := store.Begin()
	err = util.DelKeyWithPrefix(txn, t.RecordPrefix())
	if err != nil {
		return errors.Trace(err)
	}
	txn.Commit()
	return nil
}

// UpdateRecord implements table.Table UpdateRecord interface.
func (t *MemoryTable) UpdateRecord(ctx context.Context, h int64, oldData []interface{}, newData []interface{}, touched map[int]bool) error {
	// We should check whether this table has on update column which state is write only.
	currentData := make([]interface{}, len(t.Cols()))
	copy(currentData, newData)

	// If they are not set, and other data are changed, they will be updated by current timestamp too.
	err := t.setOnUpdateData(ctx, touched, currentData)
	if err != nil {
		return errors.Trace(err)
	}

	txn, err := t.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	bs := kv.NewBufferStore(txn)
	defer bs.Release()

	// set new value
	if err = t.setNewData(bs, h, touched, currentData); err != nil {
		return errors.Trace(err)
	}

	err = bs.SaveTo(txn)
	if err != nil {
		return errors.Trace(err)
	}

	err = txn.Commit()
	return nil
}

func (t *MemoryTable) setOnUpdateData(ctx context.Context, touched map[int]bool, data []interface{}) error {
	ucols := column.FindOnUpdateCols(t.Cols())
	for _, col := range ucols {
		if !touched[col.Offset] {
			value, err := expression.GetTimeValue(ctx, expression.CurrentTimestamp, col.Tp, col.Decimal)
			if err != nil {
				return errors.Trace(err)
			}

			data[col.Offset] = value
			touched[col.Offset] = true
		}
	}
	return nil
}

// SetColValue implements table.Table SetColValue interface.
func (t *MemoryTable) SetColValue(rm kv.RetrieverMutator, key []byte, data interface{}) error {
	v, err := t.EncodeValue(data)
	if err != nil {
		return errors.Trace(err)
	}
	if err := rm.Set(key, v); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (t *MemoryTable) setNewData(rm kv.RetrieverMutator, h int64, touched map[int]bool, data []interface{}) error {
	for _, col := range t.Cols() {
		if !touched[col.Offset] {
			continue
		}

		k := t.RecordKey(h, col)
		if err := t.SetColValue(rm, k, data[col.Offset]); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// AddRecord implements table.Table AddRecord interface.
func (t *MemoryTable) AddRecord(ctx context.Context, r []interface{}) (recordID int64, err error) {
	var hasRecordID bool
	for _, col := range t.Cols() {
		if col.IsPKHandleColumn(t.meta) {
			recordID, err = types.ToInt64(r[col.Offset])
			if err != nil {
				return 0, errors.Trace(err)
			}
			hasRecordID = true
			break
		}
	}
	if !hasRecordID {
		recordID, err = t.alloc.Alloc(t.ID)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	//txn, err := ctx.GetTxn(false)
	txn, err := t.store.Begin()
	if err != nil {
		return 0, errors.Trace(err)
	}
	bs := kv.NewBufferStore(txn)
	defer bs.Release()

	if err = t.LockRow(ctx, recordID); err != nil {
		return 0, errors.Trace(err)
	}

	// Set public and write only column value.
	for _, col := range t.Cols() {
		if col.IsPKHandleColumn(t.meta) {
			continue
		}
		value := r[col.Offset]
		key := t.RecordKey(recordID, col)
		err = t.SetColValue(txn, key, value)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	if err = bs.SaveTo(txn); err != nil {
		return 0, errors.Trace(err)
	}
	if ctx != nil {
		variable.GetSessionVars(ctx).AddAffectedRows(1)
	}
	txn.Commit()
	return recordID, nil
}

// EncodeValue implements table.Table EncodeValue interface.
func (t *MemoryTable) EncodeValue(raw interface{}) ([]byte, error) {
	v, err := t.flatten(raw)
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := codec.EncodeValue(nil, v)
	return b, errors.Trace(err)
}

// DecodeValue implements table.Table DecodeValue interface.
func (t *MemoryTable) DecodeValue(data []byte, col *column.Col) (interface{}, error) {
	values, err := codec.Decode(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return t.unflatten(values[0], col)
}

// RowWithCols implements table.Table RowWithCols interface.
func (t *MemoryTable) RowWithCols(retriever kv.Retriever, h int64, cols []*column.Col) ([]interface{}, error) {
	retriever, _ = t.store.Begin()
	v := make([]interface{}, len(cols))
	for i, col := range cols {
		k := t.RecordKey(h, col)
		data, err := retriever.Get(k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		val, err := t.DecodeValue(data, col)
		if err != nil {
			return nil, errors.Trace(err)
		}
		v[i] = val
	}
	return v, nil
}

// Row implements table.Table Row interface.
func (t *MemoryTable) Row(ctx context.Context, h int64) ([]interface{}, error) {
	// TODO: we only interested in mentioned cols
	//txn, err := ctx.GetTxn(false)
	txn, err := store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}

	r, err := t.RowWithCols(txn, h, t.Cols())
	if err != nil {
		return nil, errors.Trace(err)
	}
	txn.Commit()
	return r, nil
}

// LockRow implements table.Table LockRow interface.
func (t *MemoryTable) LockRow(ctx context.Context, h int64) error {
	//txn, err := ctx.GetTxn(false)
	txn, err := t.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	// Get row lock key
	lockKey := t.RecordKey(h, nil)
	// set row lock key to current txn
	err = txn.Set(lockKey, []byte(txn.String()))
	if err != nil {
		return errors.Trace(err)
	}
	txn.Commit()
	return nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (t *MemoryTable) RemoveRecord(ctx context.Context, h int64, r []interface{}) error {
	err := t.removeRowData(ctx, h)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (t *MemoryTable) removeRowData(ctx context.Context, h int64) error {
	if err := t.LockRow(ctx, h); err != nil {
		return errors.Trace(err)
	}
	//txn, err := ctx.GetTxn(false)
	txn, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	// Remove row's colume one by one
	for _, col := range t.Columns {
		k := t.RecordKey(h, col)
		err = txn.Delete([]byte(k))
		if err != nil {
			if col.State != model.StatePublic && terror.ErrorEqual(err, kv.ErrNotExist) {
				// If the column is not in public state, we may have not added the column,
				// or already deleted the column, so skip ErrNotExist error.
				continue
			}

			return errors.Trace(err)
		}
	}
	// Remove row lock
	err = txn.Delete([]byte(t.RecordKey(h, nil)))
	if err != nil {
		return errors.Trace(err)
	}
	txn.Commit()
	return nil
}

// RemoveRowIndex implements table.Table RemoveRowIndex interface.
func (t *MemoryTable) RemoveRowIndex(rm kv.RetrieverMutator, h int64, vals []interface{}, idx *column.IndexedCol) error {
	return nil
}

// BuildIndexForRow implements table.Table BuildIndexForRow interface.
func (t *MemoryTable) BuildIndexForRow(rm kv.RetrieverMutator, h int64, vals []interface{}, idx *column.IndexedCol) error {
	return nil
}

// IterRecords implements table.Table IterRecords interface.
func (t *MemoryTable) IterRecords(retriever kv.Retriever, startKey kv.Key, cols []*column.Col,
	fn table.RecordIterFunc) error {
	var err error
	retriever, err = store.Begin()
	it, err := retriever.Seek(startKey)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}

	log.Debugf("startKey:%q, key:%q, value:%q", startKey, it.Key(), it.Value())

	prefix := t.RecordPrefix()
	for it.Valid() && it.Key().HasPrefix(prefix) {
		// first kv pair is row lock information.
		// TODO: check valid lock
		// get row handle
		handle, err := DecodeRecordKeyHandle(it.Key())
		if err != nil {
			return errors.Trace(err)
		}

		data, err := t.RowWithCols(retriever, handle, cols)
		if err != nil {
			return errors.Trace(err)
		}
		more, err := fn(handle, data, cols)
		if !more || err != nil {
			return errors.Trace(err)
		}

		rk := t.RecordKey(handle, nil)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// AllocAutoID implements table.Table AllocAutoID interface.
func (t *MemoryTable) AllocAutoID() (int64, error) {
	return t.alloc.Alloc(t.ID)
}
