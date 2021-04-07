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

func convertToKeyValueItems(cache Cache) []index.Command {
	items := make([]index.Command, 0, cache.Size())
	for _, key := range cache.Keys() {
		value, _ := cache.Get(key)
		v := value.(index.Command)
		items = append(items, v)
	}

	return items
}

func (s *SsStore) Flush() {
	log.Infof("Writing %d items from memcache into new ss table.", s.cache.Size())
	items := convertToKeyValueItems(s.cache)
	str, err := s.blockStorage.WriteKvItems(items)
	if err != nil {
		log.Fatalf("Could not flush items into new ss table.", err)
	}

	log.Info("Created new index store.")
	s.blockStorage = str
	s.cache = NewMemTableCache()
	log.Info("Written items from memcache into new ss table.")
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
		log.Infof("Created new cache, size is %d", s.cache.Size())
	}

	log.Infof("Adding key %s to cache.", key)
	kv := index.NewKeyValueItem(key, value)
	cmd := index.Command{PUT_COMMAND, kv}
	s.cache.Add(key, cmd)
	return nil
}

func (s *SsStore) Get(key string) (value string, ok bool) {
	v, ok := s.cache.Get(key)

	if ok {
		log.Infof("Key %s found in cache.", key)
		value, _ = v.(string)
		return value, ok
	}

	log.Infof("Key %s not found in cache, reading block.", key)
	block, err := s.blockStorage.ReadBlock(key)
	if err != nil {
		log.Fatal("Could not load block", err)
	}
	log.Info("Block loaded.")

	value, ok = block.Get(key)
	return value, ok
}

func (s *SsStore) Del(key string) {
	kv := index.NewKeyValueItem(key, "")
	cmd := index.Command{DEL_COMMAND, kv}

	s.cache.Add(key, cmd)
}

func NewSsStore(dataPath string) (Store, error) {
	cache := NewMemTableCache()
	storage := index.NewSsBlockStorage(dataPath)

	store := SsStore{storage, cache}

	log.Info("Created new SsStore")
	return &store, nil
}
