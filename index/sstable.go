package index

import (
	"crypto/sha1"
	"encoding/csv"
	"fmt"
	"github.com/elliotchance/orderedmap"
	lru "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sort"
	"strconv"
)

const (
	BlockSizeBytes int64  = 4000
	KeySizeChar    int    = 8
	GET_COMMAND    string = "get"
	PUT_COMMAND    string = "put"
	DEL_COMMAND    string = "del"
)

type Command struct {
	Type string
	Item KeyValueItem
}

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

func (b *Block) GetH(key string) (value string, ok bool) {
	hk := key
	v, ok := b.items.Get(hk)
	var kv KeyValueItem
	if ok {
		log.Info("Key found in block")
		kv, ok = v.(KeyValueItem)
	}

	if ok {
		value = kv.Value()
	}

	return value, ok
}

func (b *Block) Get(key string) (value string, ok bool) {
	hk := keyHash(key)
	v, ok := b.items.Get(hk)
	var kv KeyValueItem
	if ok {
		log.Info("Key found in block")
		kv, ok = v.(KeyValueItem)
	}

	if ok {
		value = kv.Value()
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
	ReadBlock(key string) (block *Block, err error)
	WriteKvItems(commands []Command) (BlockStorage, error)
	RangeSearch(key1 string, key2 string) (values []string, err error)
}

type SsBlockStorage struct {
	filePath   string
	index      []string
	blockCache lru.ARCCache
}

func newSsBlockStorage(filepath string, index []string) BlockStorage {
	cacheSize := 3 * BlockSizeBytes
	var cache *lru.ARCCache
	cache, err := lru.NewARC(int(cacheSize))

	if err != nil {
		log.Fatal(err)
	}

	return &SsBlockStorage{filepath, index, *cache}
}

func searchIndex(index []string, key string) (offset int64) {
	h := keyHash(key)
	for i, key := range index {
		if i%2 == 0 && key > h {
			break
		}

		if i%2 == 0 {
			offI := i + 1
			offset, _ = strconv.ParseInt(index[offI], 10, 64)
		}
	}

	return offset
}

func readBlock(filePath string, offset int64) (block *Block, err error) {
	csvfile, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Could not open csvfile", err)
	}

	defer csvfile.Close()

	_, err = csvfile.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	log.Info("Reading block line that holds index.")
	r := csv.NewReader(csvfile)
	record, err := r.Read()
	if err != nil {
		return nil, err
	}
	log.Info("Record is read from block offset.")

	var blockKey string
	var om *orderedmap.OrderedMap = orderedmap.NewOrderedMap()
	for i, _ := range record {
		if i == 0 {
			blockKey = record[i+1]
		}

		if i == 0 || i%3 == 0 {
			size, err := strconv.ParseInt(record[i], 10, 64)
			if err != nil {
				return nil, err
			}

			key := record[i+1]
			log.Infof("Reading in kv item %s", key)
			value := record[i+2]
			kv := KeyValueItem{"", key, value, size}
			om.Set(key, kv)
		}
	}

	block = &Block{blockKey, *om, BlockSizeBytes}
	return block, nil
}

func (s *SsBlockStorage) ReadBlock(key string) (block *Block, err error) {
	log.Infof("Reading block that contains key %s, hash is %s", key, keyHash(key))
	offset := searchIndex(s.index, key)

	log.Infof("Found block index is %d", offset)

	b, ok := s.blockCache.Get(offset)
	if ok {
		log.Info("Block found in block cache.")
		block, _ = b.(*Block)
		return block, err
	}
	return readBlock(s.filePath, offset)
}

func searchIndexRange(index []string, key1 string, key2 string) (offsets []int64) {
	h1 := keyHash(key1)
	h2 := keyHash(key2)
	if h2 > h1 {
		tmp := h1
		h1 = h2
		h2 = tmp
	}

	var offset int64
	for i, key := range index {
		if i%2 != 0 {
			continue
		}

		if key >= h1 || key <= h2 {
			offI := i + 1
			offset, _ = strconv.ParseInt(index[offI], 10, 64)
			offsets = append(offsets, offset)
		}
	}

	return offsets

}
func (s *SsBlockStorage) RangeSearch(key1 string, key2 string) (values []string, err error) {
	log.Infof("Searching index for blocks that contain keys between %s and %s.", key1, key2)
	offsets := searchIndexRange(s.index, key1, key2)
	log.Infof("Found %d blocks that contain keys between %s and %s", len(offsets), key1, key2)
	for _, offs := range offsets {
		log.Infof("Reading in block.")
		block, err := readBlock(s.filePath, offs)
		if err != nil {
			return values, err
		}

		h1 := keyHash(key1)
		h2 := keyHash(key2)
		if h2 > h1 {
			log.Info("Keys hash cause out of order search, adjusting.")
			tmp := h1
			h1 = h2
			h2 = tmp
		}

		log.Infof("Checking if key from read blocks falls inclusively between keys %s and %s", key1, key2)
		for _, key := range block.Keys() {
			if key >= h1 || key <= h2 {
				value, _ := block.GetH(key)
				log.Infof("Scan value is %s", value)

				values = append(values, value)
			}
		}
		log.Infof("Checked keys inbetween %s and %s, current list of values is %d", key1, key2, len(values))
	}

	return values, nil
}

func loadIndex(filePath string) []string {
	log.Infof("Loading index from %s", filePath)
	ind := make([]string, 0, 0)
	csvfile, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Could not open csvfile", err)
	}
	defer csvfile.Close()
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

	cacheSize := 3 * BlockSizeBytes
	var cache *lru.ARCCache
	cache, _ = lru.NewARC(int(cacheSize))

	return &SsBlockStorage{filePath, ind, *cache}
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

func getIndexOffsets(index []string) (offsets []int64) {
	for i, _ := range index {
		if i%2 == 0 {
			offI := i + 1
			offset, _ := strconv.ParseInt(index[offI], 10, 64)
			offsets = append(offsets, offset)
		}
	}

	return offsets
}

func collectItemsToWrite(s SsBlockStorage, commands []Command) []KeyValueItem {
	log.Info("Collecting items to write to new sstable.")
	var items []KeyValueItem
	itemMap := make(map[string]KeyValueItem)
	offsets := getIndexOffsets(s.index)

	log.Info("reading blocks for collection")
	var blocks []Block
	for _, offset := range offsets {
		block, err := readBlock(s.filePath, offset)
		if err != nil {
			log.Fatal(err)
		}

		blocks = append(blocks, *block)
	}

	log.Info("read blocks for collection")

	log.Info("Collecting key value items from blocks.")
	for _, block := range blocks {
		for _, k := range block.Keys() {
			key := k
			value, _ := block.GetH(key)
			it := NewKeyValueItem(key, value)
			it.keyHash = key
			it.key = ""
			itemMap[key] = it
		}
	}

	log.Info("Collected key value items from blocks.")

	log.Info("Pruning commands due to delete tombstones")
	for _, cmd := range commands {
		if cmd.Type == DEL_COMMAND {
			log.Infof("Delete command found for key %s, removing from items to write.", cmd.Item.keyHash)
			delete(itemMap, cmd.Item.KeyHash())
		} else {
			itemMap[cmd.Item.KeyHash()] = cmd.Item
		}
	}

	log.Info("Pruned commands due to delete tombstones")

	for _, value := range itemMap {
		items = append(items, value)
	}

	log.Info("Collected items to write to new sstable.")
	return items
}

func (s *SsBlockStorage) WriteKvItems(commands []Command) (BlockStorage, error) {
	log.Info("Sorting key value items for write.")

	items := collectItemsToWrite(*s, commands)
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
