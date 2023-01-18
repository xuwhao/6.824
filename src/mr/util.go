package mr

import (
	"fmt"
	"strings"
	"sync"
)

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

func (c *ConcurrentMap[T, E]) String() string {
	var build strings.Builder
	//build.WriteString(s1)
	//build.WriteString(s2)
	//s3 := build.String()
	build.WriteString("{")
	c.Lock.RLock()
	for k, v := range c.Map {
		build.WriteString(fmt.Sprintf("{%d: %+v}, ", k, v))
	}
	c.Lock.RUnlock()
	build.WriteString("}")
	return build.String()
}
