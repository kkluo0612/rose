package list

import (
	"container/list"
	"reflect"

	"github.com/roseduan/rosedb/storage"
)

type Insertoption uint8

type dumpFunc func(e *storage.Entry) error

const (
	Before Insertoption = iota
	After
)

type (
	List struct {
		record Record
	}
	Record map[string]*list.List
)

func New() *List {
	return &List{
		make(Record),
	}
}

func (list *List) DumpIterate(fn dumpFunc) (err error) {
	for key, l := range list.record {
		listKey := []byte(key)
		for e := l.Front(); e != nil; e = e.Next() {
			value, _ := e.Value.([]byte)
			ent := storage.NewEntryNoExtra(listKey, value, 1, 1)
			if err = fn(ent); err != nil {
				return
			}
		}
	}
	return
}

func (list *List) LPush(key string, value ...[]byte) int {
	return list.push(true, key, value...)
}

func (list *List) LPop(key string) []byte {
	return list.pop(true, key)
}

func (list *List) RPush(key string, value ...[]byte) int {
	return list.push(false, key, value...)
}

func (list *List) RPop(key string) []byte {
	return list.pop(false, key)
}

func (list *List) LIndex(key string, index int) []byte {
	ok, newIndex := list.validIndex(key, index)
	if !ok {
		return nil
	}

	index = newIndex
	var val []byte
	e := list.index(key, index)
	if e != nil {
		val = e.Value.([]byte)
	}
	return val
}

func (list *List) LRemove(key string, value []byte, count int) int {
	item := list.record[key]
	if item == nil {
		return 0
	}
	var ele []*list.Element
	if count == 0 {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), value) {
				ele = append(ele, p)
			}
		}
	}
	if count > 0 {
		for p := item.Front(); p != nil && len(ele) < count; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), value) {
				ele = append(ele, p)
			}
		}
	}
	if count < 0 {
		for p := item.Front(); p != nil && len(ele) < -count; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), value) {
				ele = append(ele, p)
			}
		}
	}
	for _, e := range ele {
		item.Remove(e)
	}
	length := len(ele)
	ele = nil
	return length
}

func (list *List) LInsert(key string, option Insertoption, pivot, value []byte) int {
	e := list.find(key, pivot)
	if e == nil {
		return -1
	}
	item := list.record[key]
	if option == Before {
		item.InsertBefore(value, e)
	}
	if option == After {
		item.InsertAfter(value, e)
	}
	return item.Len()
}

func (list *List) LSet(key string, index int, value []byte) bool {
	e := list.index(key, index)
	if e == nil {
		return false
	}
	e.Value = value
	return true
}

func (list *List) LRange(key string, start, end int) [][]byte {
	var val [][]byte
	item := list.record[key]
	if item == nil || item.Len() < 0 {
		return val
	}

	length := item.Len()
	start, end = list.handleIndex(length, start, end)

	if start > end || start >= length {
		return val
	}

	mid := length >> 1
	if end <= mid || end-mid < mid-start {
		flag := 0
		for p := item.Front(); p != nil && flag >= start; p, flag = p.Prev(), flag-1 {
			if flag <= end {
				val = append(val, p.Value.([]byte))
			}
		}
	} else {
		flag := length - 1
		for p := item.Back(); p != nil && flag >= start; p, flag = p.Prev(), flag-1 {
			if flag <= end {
				val = append(val, p.Value.([]byte))
			}
		}
		if len(val) > 0 {
			for i, j := 0, len(val)-1; i < j; i, j = i+1, j-1 {
				val[i], val[j] = val[j], val[i]
			}
		}
	}
	return val
}

func (list *List) Ltrim(key string, start, end int) bool {
	item := list.record[key]
	if item == nil || item.Len() <= 0 {
		return false
	}

	length := item.Len()
	start, end = list.handleIndex(length, start, end)

	if start <= 0 && end >= length-1 {
		return false
	}
	if start > end || start >= length {
		list.record[key] = nil
		return false
	}
	startEle, endEle := list.index(key, start), list.index(key, end)
	if end-start+1 < (length >> 1) {
		newList := list.New()
		newValueMap := make(map[string]int)
		for p := startEle; p != endEle.Next(); p = p.Next() {
			newList.PushBack(p.Value)
			if p.Value != nil {
				newValueMap[string(p.Value.([]byte))] += 1
			}
		}
		item = nil
		list.record[key] = newList
	} else {
		var ele []*list.Element
		for p := item.Front(); p != startEle; p = p.Next() {
			ele = append(ele, p)
		}
		for p := item.Back(); p != endEle; p = p.Prev() {
			ele = append(ele, p)
		}
		for _, e := range ele {
			item.Remove(e)
		}
		ele = nil
	}
	return true
}

func (list *List) LLen(key string) int {
	length := 0
	if list.record[key] != nil {
		length = list.record[key].Len()
	}
	return length
}

func (list *List) LClear(key string) {
	delete(list.record, key)
}

func (list *List) LKeyExists(key string) (ok bool) {
	_, ok = list.record[key]
	return
}

func (list *List) find(key string, value []byte) *list.Element {
	item := list.record[key]
	var e *list.Element
	if item != nil {
		for p := item.Front(); p != nil; p = p.Next() {
			if reflect.DeepEqual(p.Value.([]byte), value) {
				e = p
				break
			}
		}
	}
	return e
}

func (list *List) index(key string, index int) *list.Element {
	ok, newIndex := list.validIndex(key, index)
	if !ok {
		return nil
	}
	index = newIndex
	item := list.record[key]
	var e *list.Element
	if item != nil && item.Len() > 0 {
		if index <= (item.Len() >> 1) {
			val := item.Front()
			for i := 0; i < index; i++ {
				val = val.Next()
			}
			e = val
		} else {
			val := item.Back()
			for i := item.Len() - 1; i > index; i-- {
				val = val.Prev()
			}
			e = val
		}
	}
	return e
}

func (list *List) push(front bool, key string, value ...[]byte) int {
	if list.record[key] == nil {
		list.record[key] = list.New()
	}
	for _, v := range value {
		if front {
			list.record[key].PushFront(v)
		} else {
			list.record[key].PushBack(v)
		}
	}
	return list.record[key].Len()
}

func (list *List) pop(front bool, key string) []byte {
	item := list.record[key]
	var val []byte
	if item != nil && item.Len() > 0 {
		var e *list.Element
		if front {
			e = item.Front()
		} else {
			e = item.Back()
		}
		val = e.Value.([]byte)
		item.Remove(e)
	}
	return val
}

func (list *List) validIndex(key string, index int) (bool, int) {
	item := list.record[key]
	if item != nil && item.Len() <= 0 {
		return false, index
	}
	length := item.Len()
	if index < 0 {
		index += length
	}
	return index >= 0 && index < length, index
}

func (list *List) handleIndex(length, start, end int) (int, int) {
	if start < 0 {
		start += length
	}
	if end < 0 {
		end += length
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	return start, end
}
