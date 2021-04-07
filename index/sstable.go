package index

import (
	"crypto/sha1"
	"encoding/csv"
	"fmt"
	"github.com/elliotchance/orderedmap"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sort"
)

const (
	BlockSizeBytes int64 = 72
	KeySizeChar    int   = 8
)

func keyHash(key string) string {
	h := sha1.New()
	h.Write([]byte(key))
	b := h.Sum(nil)
	hashString := fmt.Sprintf("%x", b)
	return hashString[0:KeySizeChar]
}

type KeyValueItem struct {
	key     string
	keyHash string
	value   string
	size    int64
}

func (k *KeyValueItem) KeyHash() string {
	return k.keyHash
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
	s := KeySizeChar + len([]byte(value))
	size := int64(s)
	kh := keyHash(key)
	return KeyValueItem{key, kh, value, size}
}

type Block struct {
	blockKey string
	items    orderedmap.OrderedMap
	size     int64
}

func (b *Block) BlockKey() string {
	return b.blockKey
}

func (b *Block) Keys() []string {
	keys := make([]string, 0, b.items.Len())
	for _, k := range b.items.Keys() {
		key, ok := k.(string)
		if ok {
			keys = append(keys, key)
		}
	}

	return keys
}

func (b *Block) Get(key string) (value string, ok bool) {
	v, ok := b.items.Get(key)
	if ok {
		value, ok = v.(string)
	}

	return value, ok
}

func (b *Block) Size() int64 {
	return b.size
}

func NewBlock(blockKey string, items orderedmap.OrderedMap) Block {
	return Block{blockKey, items, BlockSizeBytes}
}

type BlockStorage interface {
	//ReadBlock(key string) (block Block, err error)
	WriteKvItems(items []KeyValueItem) (BlockStorage, error)
}

type SsBlockStorage struct {
	filePath string
	index    []string
}

func newSsBlockStorage(filepath string, index []string) BlockStorage {
	return &SsBlockStorage{filepath, index}
}

func loadIndex(filePath string) []string {
	log.Infof("Loading index from %s", filePath)
	ind := make([]string, 0, 0)
	csvfile, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Could not open csvfile", err)
	}

	log.Info("Reading second line that holds index.")
	r := csv.NewReader(csvfile)
	r.FieldsPerRecord = -1
	var rec []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		rec = record
	}

	log.Info("Second line retrieved, parsing index.")
	for i, key := range rec {
		if i%2 == 0 {
			offI := i + 1
			offset := rec[offI]
			log.Infof("Adding key %s and offset %s to index.", key, offset)
			ind = append(ind, key)
			ind = append(ind, offset)
		}
	}

	log.Info("Index is loaded.")
	return ind
}

func NewSsBlockStorage(filePath string) BlockStorage {
	ind := make([]string, 0, 0)
	_, err := os.Stat(filePath)
	if err == nil {
		log.Info("Existing data file detected loading in index.")
		ind = loadIndex(filePath)
	} else {
		log.Info("No data file detected using empty index.")
	}
	return &SsBlockStorage{filePath, ind}
}

type By func(i1, i2 *KeyValueItem) bool

func (by By) Sort(items []KeyValueItem) {
	it := &KeyValueItemSorter{
		items: items,
		by:    by,
	}
	sort.Sort(it)
}

type KeyValueItemSorter struct {
	items []KeyValueItem
	by    func(i1, i2 *KeyValueItem) bool
}

func (k *KeyValueItemSorter) Len() int {
	return len(k.items)
}

func (k *KeyValueItemSorter) Swap(i, j int) {
	k.items[i], k.items[j] = k.items[j], k.items[i]
}

func (k *KeyValueItemSorter) Less(i, j int) bool {
	return k.by(&k.items[i], &k.items[j])
}

func sortKeyValueItemsByHash(items []KeyValueItem) {
	hsh := func(i1, i2 *KeyValueItem) bool {
		return i1.keyHash < i2.keyHash
	}

	By(hsh).Sort(items)
}

func keyValueItemsOrderedMap(items []KeyValueItem) *orderedmap.OrderedMap {
	m := orderedmap.NewOrderedMap()
	for _, it := range items {
		log.Infof("Adding kv item with key %s to ordered hash", it.KeyHash())
		m.Set(it.KeyHash(), it)
	}

	return m
}

// items are assumed ordered
func createBlock(items []KeyValueItem, startingIndex int) (block Block, nextIndex int) {
	var currentSizeBytes int64 = 0
	endIndex := startingIndex
	log.Infof("Calculating indexes from items of length %d, to create block.", len(items))
	first := true

	// minus one is for newline
	for endIndex < len(items) && currentSizeBytes+items[endIndex].Size() <= BlockSizeBytes-1 {
		it := items[endIndex]
		meta := 3
		if first {
			meta = 2
			first = false
		}

		currentSizeBytes += (it.Size() + int64(meta))
		endIndex += 1
	}

	log.Info("Calculated indexes to create block.")
	log.Info("Creating ordered map for block.")
	m := keyValueItemsOrderedMap(items[startingIndex:endIndex])
	log.Info("Created ordered map for block.")
	block = NewBlock(items[startingIndex].keyHash, *m)
	nextIndex = endIndex

	return block, endIndex
}

func getLastIndex(filepath string) (offset int64, err error) {
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return -1, err
	}

	defer f.Close()

	offset, err = f.Seek(0, io.SeekEnd)
	return offset, err
}

func writeBlock(filepath string, block Block) (offset int64, err error) {
	offset, err = getLastIndex(filepath)
	if err != nil {
		return -1, err
	}

	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	firstRecord := true
	for _, k := range block.Keys() {
		i, _ := block.items.Get(k)
		it, _ := i.(KeyValueItem)

		var s string
		if firstRecord {
			s = fmt.Sprintf("%d,%s,%s", it.Size(), it.KeyHash(), it.Value())
			firstRecord = false
		} else {
			s = fmt.Sprintf(",%d,%s,%s", it.Size(), it.KeyHash(), it.Value())
		}
		_, werr := f.WriteString(s)
		if werr != nil {
			return -1, werr
		}
	}

	_, werr := f.WriteString("\n")
	if werr != nil {
		return -1, werr
	}

	return offset, nil
}

func writeIndex(filepath string, index []string) error {
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer f.Close()

	indexString := ""
	firstItem := true
	for _, indexItem := range index {
		if firstItem {
			indexString = fmt.Sprintf("%s%s", indexString, indexItem)
			firstItem = false
		} else {
			indexString = fmt.Sprintf("%s,%s", indexString, indexItem)
		}
	}

	_, err = f.WriteString(indexString)

	return err
}

func deleteSsFile(filepath string) error {
	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		log.Infof("file at %s, does not exist. no removal needed.", filepath)
		return nil
	} else {
		return os.Remove(filepath)
	}
}

func (s *SsBlockStorage) WriteKvItems(items []KeyValueItem) (BlockStorage, error) {
	log.Info("Sorting key value items for write.")
	sortKeyValueItemsByHash(items)
	log.Info("Key value items sorted for write.")
	startingIndex := 0

	log.Info("Removing old sstable file if exists.")
	tmpFilePath := "./temp_data.txt"

	index := make([]string, 0, 5000)
	for startingIndex < len(items) {
		block, nextIndex := createBlock(items, startingIndex)
		startingIndex = nextIndex
		log.Infof("Created block %s, next index of items are %d", block.BlockKey(), startingIndex)
		off, err := writeBlock(tmpFilePath, block)
		index = append(index, block.BlockKey())
		index = append(index, fmt.Sprintf("%d", off))
		if err != nil {
			log.Errorf("Unable to write block %s", block.BlockKey())
			return nil, err
		}

		log.Infof("Block %s is written", block.BlockKey())
	}

	err := writeIndex(tmpFilePath, index)
	if err != nil {
		log.Errorf("Unable to write index to file %s.", s.filePath)
		return nil, err
	}

	log.Info("Swapping old data file with new one.")
	err = os.Rename(tmpFilePath, s.filePath)
	if err != nil {
		log.Fatal("Could not swap data files.")
		return nil, err
	}

	log.Info("Index written to file. Creating new Block storage to return.")
	var storage BlockStorage = newSsBlockStorage(s.filePath, index)
	return storage, nil
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
