package common

import (
	"fmt"
	"golang.org/x/sync/errgroup"
	"slices"
	"sync"
)

type EventBus[KeyType comparable] struct {
	subscribers   map[KeyType][]chan any
	messages      map[KeyType][]any
	notifier      sync.Cond
	notifierWLock sync.Locker
}

func NewEventBus[KeyType comparable]() *EventBus[KeyType] {
	notifierLock := sync.RWMutex{}

	return &EventBus[KeyType]{
		subscribers:   make(map[KeyType][]chan any),
		messages:      make(map[KeyType][]any),
		notifier:      sync.Cond{L: notifierLock.RLocker()},
		notifierWLock: &notifierLock,
	}
}

// Subscribe returns a channel that will receive messages when the key is published.
// If channelBufferSize is 0, the channel is unbuffered;
// if channelBufferSize is -1, the channel is unlimited-buffered.
func (eb *EventBus[KeyType]) Subscribe(key KeyType, channelBufferSize int) chan any {
	ch := make(chan any, channelBufferSize)
	eb.notifierWLock.Lock()
	eb.subscribers[key] = append(eb.subscribers[key], ch)
	if _, exist := eb.messages[key]; !exist {
		eb.messages[key] = make([]any, 0)
	}
	eb.notifierWLock.Unlock()

	// coroutine listening to new (and old) messages and sending through the channel.
	go func() {
		curMessagePos := 0
		for {
			eb.notifier.L.Lock()
			// if the key is removed, the key has been unpublished,
			// so we should stop listening.
			if _, exist := eb.messages[key]; !exist {
				eb.notifier.L.Unlock()
				break
			}
			for curMessagePos >= len(eb.messages[key]) {
				eb.notifier.Wait()
			}

			// finally, new messages are coming!
			var eg errgroup.Group
			eg.Go(func() (e error) {
				defer func() {
					if r := recover(); r != nil {
						e = fmt.Errorf("panic: %v", r)
					}
				}()
				for _, message := range eb.messages[key][curMessagePos:] {
					ch <- message
				}
				e = nil
				return
			})
			err := eg.Wait()
			if err != nil {
				// if the channel is closed, we should stop listening.
				eb.notifier.L.Unlock()
				break
			}
			curMessagePos = len(eb.messages[key])
			eb.notifier.L.Unlock()
		}
	}()
	return ch
}

func (eb *EventBus[KeyType]) Unsubscribe(key KeyType, ch chan any) {
	eb.notifierWLock.Lock()
	defer eb.notifierWLock.Unlock()
	if i := slices.Index(eb.subscribers[key], ch); i >= 0 {
		eb.subscribers[key], _ = RemoveUnordered(eb.subscribers[key], i)
		close(ch)
	}
}

func (eb *EventBus[KeyType]) Publish(key KeyType, message any) {
	eb.notifierWLock.Lock()
	eb.messages[key] = append(eb.messages[key], message)
	eb.notifierWLock.Unlock()
	eb.notifier.Broadcast()
}

func (eb *EventBus[KeyType]) ClosePublish(key KeyType) {
	eb.notifierWLock.Lock()
	delete(eb.messages, key)
	for _, ch := range eb.subscribers[key] {
		close(ch)
	}
	eb.notifierWLock.Unlock()
	// optional, but it's better to trigger the coroutines so that they stop listening.
	eb.notifier.Broadcast()
}
