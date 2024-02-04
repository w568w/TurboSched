package common

import (
	"sync"

	"gorm.io/gorm"
)

// The generic interface for a pool item.
type PoolItem[IdType any] interface {
	// Get the item id.
	GetId() IdType
	// Get the database pointer object (i.e. we can call db.Create(item.getDBObj()) to save it to database)
	AsDBPtr() any
}

// A task pool backed by a database.
type TaskPool struct {
	database  *gorm.DB
	schedCond *sync.Cond
}

type Task PoolItem[uint64]

func NewTaskPool(db *gorm.DB, schedCond *sync.Cond) *TaskPool {
	return &TaskPool{
		database:  db,
		schedCond: schedCond,
	}
}

// Put a task to the pool.
func (q *TaskPool) Put(task Task) (uint64, error) {
	q.schedCond.L.Lock()
	defer q.schedCond.L.Unlock()
	result := q.database.Create(task.AsDBPtr())
	if result.Error != nil {
		return 0, result.Error
	}
	q.schedCond.Signal()
	return task.GetId(), nil
}

// A device pool backed by a database.
type DevicePool struct {
	database  *gorm.DB
	schedCond *sync.Cond
}

type Device PoolItem[string]

func NewDevicePool(db *gorm.DB, schedCond *sync.Cond) *DevicePool {
	return &DevicePool{
		database:  db,
		schedCond: schedCond,
	}
}

// Put a device to the pool.
func (q *DevicePool) Put(device Device) error {
	q.schedCond.L.Lock()
	defer q.schedCond.L.Unlock()
	result := q.database.Save(device.AsDBPtr())
	if result.Error != nil {
		q.schedCond.Signal()
	}
	return result.Error
}
