package store

import (
	"github.com/shimanekb/project2-A/index/sstable"
	log "github.com/sirupsen/logrus"
)

const (
	DATA_FLUSH_THRESHOLD int    = 5
	GET_COMMAND          string = "get"
	PUT_COMMAND          string = "put"
	DEL_COMMAND          string = "del"
)

type Command struct {
	Type  string
	Key   string
	Value string
}

type Store interface {
	Put(key string, value string) error
	Get(key string) (value string, ok bool)
	Del(key string)
	Flush()
}

type SsStore struct {
	blockStorage sstable.BlockStorage
	cache        Cache
}

func convertToKeyValueItems(cache Cache) []sstable.KeyValueItem {
	items := make([]sstable.KeyValueItem, 0, cache.Size())
	for _, key := range cache.Keys() {
		value, _ := cache.Get(key)
		v := value.(string)
		kv := sstable.NewKeyValueItem(key, v)
		items = append(items, kv)
	}

	return items
}

func (s *SsStore) Put(key string, value string) error {
	if s.cache.Size() >= DATA_FLUSH_THRESHOLD {
		log.Info("Data threshold met, creating new sstable store.")
		items := convertToKeyValueItems(s.cache)
		str, err := s.blockStorage.WriteKvItems(items)

		if err != nil {
			return err
		}

		log.Info("Created new sstable store.")
		s.blockStorage = str
		s.cache, _ = NewMemTableCache()
	}

	s.cache.Add(key, value)
	return nil
}

func (s *SsStore) Get(key string) (value string, ok bool) {
	return "", false
}

func (s *SsStore) Del(key string) {
}

func (s *SsStore) Flush() {
}

func NewSsStore(dataPath string) (Store, error) {
	cache, _ := NewMemTableCache()
	storage := sstable.NewSsBlockStorage(dataPath)

	store := SsStore{storage, cache}

	return &store, nil
}
