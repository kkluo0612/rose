package hash

import (
	"github.com/roseduan/rosedb/storage"
)

type dumpFunc func(e *storage.Entry) error

type (
	Hash struct {
		record Record
	}

	Record map[string]map[string][]byte
)

func New() *Hash {
	return &Hash{make(Record)}
}

func (h *Hash) DumpIterate(fn dumpFunc) (err error) {
	for key, h := range h.record {
		hashkey := []byte(key)
		for field, value := range h {
			ent := storage.NewEntry(hashkey, value, []byte(field), 2, 0)
			if err = fn(ent); err != nil {
				return
			}
		}
	}
	return
}

func (h *Hash) HSet(key string, field string, value []byte) (res int) {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}
	if h.record[key][field] != nil {
		h.record[key][field] = value
	} else {
		h.record[key][field] = value
		res = 1
	}
	return
}

func (h *Hash) HSetNx(key string, field string, value []byte) int {
	if !h.exist(key) {
		h.record[key] = make(map[string][]byte)
	}
	if _, exist := h.record[key][field]; !exist {
		h.record[key][field] = value
		return 1
	}
	return 0
}

func (h *Hash) HGet(key, field string) []byte {
	if !h.exist(key) {
		return nil
	}
	return h.record[key][field]
}

func (h *Hash) HGetAll(key string) (res [][]byte) {
	if !h.exist(key) {
		return
	}
	for k, v := range h.record[key] {
		res = append(res, []byte(k), v)
	}
	return
}

func (h *Hash) HDel(key, field string) int {
	if !h.exist(key) {
		return 0
	}
	if _, exist := h.record[key][field]; exist {
		delete(h.record[key], field)
		return 1
	}
	return 0
}

func (h *Hash) HKeyExists(key string) bool {
	return h.exist[key]
}

func (h *Hash) HExists(key, field string) (ok bool) {
	if !h.exist(key) {
		return
	}
	if _, exist := h.record[key][field]; exist {
		ok = true
	}
	return
}

func (h *Hash) HLen(key string) int {
	if !h.exist(key) {
		return 0
	}
	return len(h.record[key])
}

func (h *Hash) HKeys(key string) (val []string) {
	if !h.exist(key) {
		return
	}

	for k := range h.record[key] {
		val = append(val, k)
	}
	return
}

func (h *Hash) HVals(key string) (val [][]byte) {
	if !h.exist(key) {
		return
	}

	for _, v := range h.record[key] {
		val = append(val, v)
	}
	return
}

func (h *Hash) HClear(key string) {
	if !h.exist(key) {
		return
	}
	delete(h.record, key)
}

func (h *Hash) exist(key string) bool {
	_, exist := h.record[key]
	return exist
}
