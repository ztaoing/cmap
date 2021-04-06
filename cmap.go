/**
* @Author:zhoutao
* @Date:2021/4/6 下午2:08
* @Desc:
 */

package cmap

import (
	"encoding/json"
	"sync"
)

//分片数为32
var SHARD_COUNT = 32

//锁在每个分片内，也就是将锁的粒度降低到单个分片中
type ConcurrentMap []*ConcurrentMapShared

type ConcurrentMapShared struct {
	items        map[string]interface{}
	sync.RWMutex //读写锁
}

//初始化分片map
func New() ConcurrentMap {
	m := make(ConcurrentMap, SHARD_COUNT)
	//为每个分片初始化线程安全的map
	for i := 0; i < SHARD_COUNT; i++ {
		m[i] = &ConcurrentMapShared{
			items: make(map[string]interface{}),
		}
	}
	return m
}

//通过key计算hash值，并获取对应的分片
func (m ConcurrentMap) GetShard(key string) *ConcurrentMapShared {
	return m[uint(fnv32(key))%uint(SHARD_COUNT)]
}

//设置多个值
func (m ConcurrentMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		//获取分片
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

//设置单个值
func (m ConcurrentMap) Set(key string, value interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

func (m ConcurrentMap) Upsert(key string, value interface{}, callback UpsertCb) (res interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = callback(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

//如果存在的话,就设置
func (m ConcurrentMap) SetIfAbsent(key string, value interface{}) bool {
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

func (m ConcurrentMap) Get(key string) (interface{}, bool) {
	shard := m.GetShard(key)
	shard.RLock()
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

//返回所有分片中数据的总个数
func (m ConcurrentMap) Count() int {
	count := 0
	for i := 0; i < SHARD_COUNT; i++ {
		shard := m[i]
		shard.Lock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

//是否存在key
func (m ConcurrentMap) Has(key string) bool {
	shard := m.GetShard(key)
	shard.RLock()
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

//移除key
func (m ConcurrentMap) Remove(key string) {
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

type RemoveCallBack func(key string, v interface{}, exists bool) bool

func (m ConcurrentMap) RemoveCallBack(key string, callback RemoveCallBack) bool {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := callback(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

//pop
func (m ConcurrentMap) Pop(key string) (v interface{}, exists bool) {
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

func (m ConcurrentMap) IsEmpty() bool {
	return m.Count() == 0
}

//元组
type Tuple struct {
	Key string
	Val interface{}
}

//返回一个迭代器，它可以用在一个range的循环中
func (m ConcurrentMap) Iter() <-chan Tuple {
	chans := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

func (m ConcurrentMap) IterBuffered() <-chan Tuple {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

//移除所有item
func (m ConcurrentMap) Clear() {
	for item := range m.IterBuffered() {
		m.Remove(item.Key)
	}
}

//返回的是一个channel的数组，它里边包括了每一个分片的所有元素，就像是对m，照了一个快照一样
func snapshot(m ConcurrentMap) (chans []chan Tuple) {
	chans = make([]chan Tuple, SHARD_COUNT)
	wg := sync.WaitGroup{}
	wg.Add(SHARD_COUNT)
	// 处理每一个分片
	for index, shard := range m {
		go func(index int, shard *ConcurrentMapShared) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			//取出shard中的所有元素，放入对应的channel中
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

//扇入,读取多个channel中的元素然后放入一个channel中
func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		//单独处理每个channel
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

//将所有元素放入到一个map中
func (m ConcurrentMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})
	// 将数据插入到当前的map中
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

type IterCallback func(key string, v interface{})

func (m ConcurrentMap) IterCallback(fn IterCallback) {
	for idx := range m {
		shard := (m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

//返回所有的key
func (m ConcurrentMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// 遍历每一个分区
		wg := sync.WaitGroup{}
		wg.Add(SHARD_COUNT)
		for _, shard := range m {
			go func(shard *ConcurrentMapShared) {
				shard.RLock()
				for key := range shard.items {
					//放入chan中
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	keys := make([]string, 0, count)
	for k := range ch {
		//从channel中取出放入到slice中
		keys = append(keys, k)
	}
	return keys
}

//json map
func (m ConcurrentMap) MarshalJSON() ([]byte, error) {
	tmp := make(map[string]interface{})
	//把item插入到临时的map中
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

//使用fnv算法计算hash值
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
