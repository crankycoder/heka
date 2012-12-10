package pipeline

import (
	"errors"
)

type Queue struct {
	queue chan interface{}

	no_item_error  error
	queue_full_err error
}

func NewQueue(size int) *Queue {
	return &Queue{queue: make(chan interface{}, size),
		no_item_error:  errors.New("No item"),
		queue_full_err: errors.New("Queue Full"),
	}
}

func (self *Queue) push(item interface{}) error {
	select {
	case self.queue <- item:
		// yay - we enqueued the item
		return nil
	default:
		return self.queue_full_err
	}
	return nil
}

func (self *Queue) nextItem() (interface{}, error) {
	var next_item interface{}

	select {
	case next_item = <-self.queue:

		return next_item, nil
	default:
		return nil, errors.New("No item")
	}
	return nil, errors.New("This is unreachable")
}
