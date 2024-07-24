package common

import (
	"sync"

	"gorm.io/gorm"
)

// PoolItem is the generic interface for a pool item.
type PoolItem[IdType any] interface {
	// GetId returns the item id.
	GetId() IdType
	// AsDBPtr returns the database pointer object (i.e. we can call db.Create(item.getDBObj()) to save it to database)
	AsDBPtr() any
}

// WithCondition is an interface for a pool that has a condition variable.
type WithCondition interface {
	// GetConditionVar returns the condition variable of the pool.
	GetConditionVar() *sync.Cond
}

// scopedAction does some custom action with a condition variable locked.
func scopedAction(action func() (needNotify bool, err error), withCond WithCondition) error {
	withCond.GetConditionVar().L.Lock()
	defer withCond.GetConditionVar().L.Unlock()
	needNotify, err := action()
	if err != nil {
		return err
	}
	if needNotify {
		withCond.GetConditionVar().Signal()
	}
	return nil
}

// TaskPool is a task pool backed by a database.
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
	err := q.Action(func() (bool, error) {
		result := q.database.Save(task.AsDBPtr())
		return true, result.Error
	})
	return task.GetId(), err
}

// implement the WithCondition interface
func (q *TaskPool) GetConditionVar() *sync.Cond {
	return q.schedCond
}

// Action does some custom action with pool locked.
func (q *TaskPool) Action(action func() (bool, error)) error {
	return scopedAction(action, q)
}

// DevicePool is a device pool backed by a database.
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
	return q.Action(func() (bool, error) {
		result := q.database.Save(device.AsDBPtr())
		return true, result.Error
	})
}

// Action does some custom action. See TaskPool.Action for more details.
func (q *DevicePool) Action(action func() (bool, error)) error {
	return scopedAction(action, q)
}

// implement the WithCondition interface
func (q *DevicePool) GetConditionVar() *sync.Cond {
	return q.schedCond
}
