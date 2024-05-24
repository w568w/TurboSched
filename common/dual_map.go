package common

type DualMap[T1 comparable, T2 comparable, V any] struct {
	key1Map map[T1]**V
	key2Map map[T2]**V
}

func NewDualMap[T1 comparable, T2 comparable, V any]() DualMap[T1, T2, V] {
	return DualMap[T1, T2, V]{
		key1Map: make(map[T1]**V),
		key2Map: make(map[T2]**V),
	}
}

func (m *DualMap[T1, T2, V]) Put(key1 T1, key2 T2, value V) {
	storedValue := &value
	m.key1Map[key1] = &storedValue
	m.key2Map[key2] = &storedValue
}

func (m *DualMap[T1, T2, V]) GetByKey1(key1 T1) (*V, bool) {
	v, ok := m.key1Map[key1]
	if !ok {
		return nil, false
	}
	if *v == nil {
		delete(m.key1Map, key1)
		return nil, false
	}
	return *v, true
}

func (m *DualMap[T1, T2, V]) DeleteByKey1(key1 T1) {
	v, ok := m.key1Map[key1]
	if ok {
		*v = nil
		delete(m.key1Map, key1)
	}
}

func (m *DualMap[T1, T2, V]) GetByKey2(key2 T2) (*V, bool) {
	v, ok := m.key2Map[key2]
	if !ok {
		return nil, false
	}
	if *v == nil {
		delete(m.key2Map, key2)
		return nil, false
	}
	return *v, true
}

func (m *DualMap[T1, T2, V]) DeleteByKey2(key2 T2) {
	v, ok := m.key2Map[key2]
	if ok {
		*v = nil
		delete(m.key2Map, key2)
	}
}
