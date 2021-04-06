package store

import (
	"github.com/shimanekb/project2-A/index"
	log "github.com/sirupsen/logrus"
)

const (
	DATA_FLUSH_THRESHOLD int    = 12
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
	blockStorage index.BlockStorage
	cache        Cache
}

func convertToKeyValueItems(cache Cache) []index.KeyValueItem {
	items := make([]index.KeyValueItem, 0, cache.Size())
	for _, key := range cache.Keys() {
		value, _ := cache.Get(key)
		v := value.(string)
		kv := index.NewKeyValueItem(key, v)
		items = append(items, kv)
	}

	return items
}

func (s *SsStore) Put(key string, value string) error {
	log.Infof("Cache size is %d", s.cache.Size())
	if s.cache.Size() >= DATA_FLUSH_THRESHOLD {
		log.Info("Data threshold met, creating new index store.")
		items := convertToKeyValueItems(s.cache)
		str, err := s.blockStorage.WriteKvItems(items)

		if err != nil {
			return err
		}

		log.Info("Created new index store.")
		s.blockStorage = str
		s.cache = NewMemTableCache()
	}

	log.Infof("Adding key %s to cache.", key)
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
	cache := NewMemTableCache()
	storage := index.NewSsBlockStorage(dataPath)

	store := SsStore{storage, cache}

	log.Info("Created new SsStore")
	return &store, nil
}
