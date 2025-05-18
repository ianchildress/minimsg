package broker

import (
	"container/list"
	"os"
	"sync"
)

// fileEntry holds one open file and its key.
type fileEntry struct {
	key  string
	file *os.File
}

// FileCache is a fixed-capacity LRU cache of *os.File handles.
type FileCache struct {
	mu      sync.Mutex
	cap     int
	ll      *list.List               // most-recent at Front, least-recent at Back
	entries map[string]*list.Element // key → *list.Element whose Value.(*fileEntry)
}

// NewFileCache creates an LRU cache that holds at most cap files.
func NewFileCache(cap int) *FileCache {
	return &FileCache{
		cap:     cap,
		ll:      list.New(),
		entries: make(map[string]*list.Element, cap),
	}
}

// Get returns the *os.File for key (e.g. a segment path), or nil if not cached.
// If found, it moves that fileEntry to the front (most-recent).
func (c *FileCache) Get(key string) *os.File {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*fileEntry).file
	}
	return nil
}

// Put opens (or moves) key→file into the cache, evicting the LRU if needed.
func (c *FileCache) Put(key string, file *os.File) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If already in cache, just promote
	if el, ok := c.entries[key]; ok {
		c.ll.MoveToFront(el)
		return
	}

	// Evict if at capacity
	if c.ll.Len() >= c.cap {
		old := c.ll.Back()
		if old != nil {
			ent := old.Value.(*fileEntry)
			ent.file.Close()           // clean up the FD
			delete(c.entries, ent.key) // drop from map
			c.ll.Remove(old)           // drop from list
		}
	}

	// Insert new fileEntry at front
	el := c.ll.PushFront(&fileEntry{key: key, file: file})
	c.entries[key] = el
}
