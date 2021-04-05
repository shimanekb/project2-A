package index

import (
	"encoding/csv"
	"errors"
	"fmt"
	order "github.com/elliotchance/orderedmap"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strconv"
)

const (
	BlockSizeBytes int64 = 4000
)

type KeyValueItem struct {
	key   string
	value string
	size  int64
}

func (k *KeyValueItem) Key() string {
	return k.key
}

func (k *KeyValueItem) Value() string {
	return k.value
}

func (k *KeyValueItem) Size() int64 {
	return k.size
}

func NewKeyValueItem(key string, value string) KeyValueItem {
	size := int64(len([]byte(value)))
	return KeyValueItem{key, value, size}
}

type Block struct {
	blockNumber int
	items       order.OrderedMap
	size        int64
}

func (b *Block) BlockNumber() int {
	return b.blockNumber
}

func (b *Block) Keys() []string {
	return b.items.Keys()
}

func (b *Block) Get(key string) (value string, ok bool) {
	return b.items.Get(key)
}

func (b *Block) Size() int64 {
	return b.size
}

func NewBlock(blockNumber int, items order.OrderMap) Block {
	return Block{blockNumber, items, BlockSizeBytes}
}

type BlockStorage interface {
	//ReadBlock(blockNumber int) (block Block, err error)
	WriteKvItems(items []KeyValueItem) error
}

type SsBlockStorage struct {
	filePath     string
	currentBlock *Block
}

func NewSsBlockStorage(filePath string) BlockStorage {
	return &SsBlockStorage{filePath, nil}
}

func (s *SsBlockStorage) WriteKvItems(items []KeyValueItem) error {
	// order items
	// if current block empty make new empty
	return nil
}

/*
type LocalDataLogReader struct {
	filePath      string
	currentOffset int64
}



type LogItem struct {
	key    string
	value  string
	size   int64
	offset int64
}

func (l *LogItem) Key() string {
	return l.key
}

func (l *LogItem) Value() string {
	return l.value
}

func (l *LogItem) Size() int64 {
	return l.size
}

func (l *LogItem) Offset() int64 {
	return l.offset
}

func NewLogItem(key string, value string, offset int64) LogItem {
	size := int64(len([]byte(value)))
	return LogItem{key, value, size, offset}
}

type LocalDataLog struct {
	flushThreshold int
	filePath       string
	buffer         []LogItem
}

func NewLocalDataLog(filePath string) DataLog {
	buffer := make([]LogItem, 0, 10)
	dataLog := LocalDataLog{10, filePath, buffer}
	return &dataLog
}

func (l *LocalDataLog) ReadLogItem(offset int64) (logItem *LogItem, err error) {
	storeFile, err := os.OpenFile(l.filePath, os.O_RDONLY, 0644)

	if _, err := os.Stat(l.filePath); os.IsNotExist(err) {
		return nil, io.EOF
	}

	if err != nil {
		log.Error(fmt.Sprintf("Unable to open data log file at %s", l.filePath), err)
		return nil, err
	}

	defer storeFile.Close()

	stat, _ := storeFile.Stat()
	if stat.Size() <= offset {
		log.Info("End of data log detected sined EOF.")
		return nil, io.EOF
	}

	_, err = storeFile.Seek(offset, 0)
	if err != nil {
		log.Error(fmt.Sprintf("Unable to seek to offset in data log file at %s", l.filePath), err)
		return nil, err
	}

	reader := csv.NewReader(storeFile)
	record, err := reader.Read()

	if err != nil {
		log.Error(fmt.Sprintf("Unable to read csv record in data log file at %s", l.filePath), err)
		return nil, err
	}

	key := record[0]
	value := record[1]
	s := record[2]
	size, parseError := strconv.ParseInt(s, 10, 64)

	if parseError != nil {
		return nil, errors.New(fmt.Sprintf("Could not convert size to int for offset %d", offset))
	}

	li := NewLogItem(key, value, offset)
	li.size = size
	return &li, nil
}

func (l *LocalDataLog) AddLogItem(logItem LogItem) (offset int64, err error) {
	log.Infof("Adding log item to %s.", l.filePath)
	file, err := os.OpenFile(l.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Errorf("Could not open data log file %s", l.filePath, err)
		return 0, err
	}

	defer file.Close()

	length, write_err := file.WriteString(fmt.Sprintf("%s,%s,%d\n", logItem.Key(), logItem.Value(), logItem.Size()))

	if write_err != nil {
		log.Errorf("Could not write log item to data log file %s", l.filePath, err)
		return 0, write_err
	}

	fi, statErr := file.Stat()
	if statErr != nil {
		log.Error("Could not get current file size to calculate new offset.", err)
		return 0, statErr
	}

	offset = fi.Size() - int64(length)
	log.Infof("Added log item at offset %d to %s.", offset, l.filePath)
	return offset, nil
}
*/
