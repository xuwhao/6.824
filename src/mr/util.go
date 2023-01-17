package mr

import "sync"

type ConcurrentMap[T comparable, E any] struct {
	Lock *sync.RWMutex
	Map  map[T]E
}

func (c *ConcurrentMap[T, E]) Add(key T, value E) {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	c.Map[key] = value
}

func (c *ConcurrentMap[T, E]) Remove(key T) {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	delete(c.Map, key)
}
func (c *ConcurrentMap[T, E]) Get(key T) (E, bool) {
	c.Lock.RLock()
	defer c.Lock.RUnlock()
	v, ok := c.Map[key]
	return v, ok
}
