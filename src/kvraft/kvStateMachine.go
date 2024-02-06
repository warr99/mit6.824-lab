package kvraft

// 内存版本的 KV 状态机 MemoryKV
// 在生产级别的 KV 服务中，数据不可能全存在内存中，系统往往采用的是 LSM 的架构，例如 RocksDB 等
// 这里后续可以实现一个 go 版本的 leveldb ?
type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}
