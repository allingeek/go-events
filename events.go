//  Copyright 2017 Jeff Nickoloff "jeff@allingeek.com"
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package events

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

const configKey = `go-events-tags`

type EventsConfig struct {
	Tags     map[string]string
	Registry *Registry
}

func NewContext(ctx context.Context, config EventsConfig) context.Context {
	ec, ok := FromContext(ctx)
	if ok && ec.Tags != nil {
		if config.Tags == nil {
			config.Tags = make(map[string]string)
		}
		for k, v := range ec.Tags {
			if _, ok = config.Tags[k]; !ok {
				config.Tags[k] = v
			}
		}
	}
	return context.WithValue(ctx, configKey, config)
}

func FromContext(ctx context.Context) (EventsConfig, bool) {
	ec, ok := ctx.Value(configKey).(EventsConfig)
	return ec, ok
}

type Registry struct {
	subscriptions map[string]chan EventBlock
	m             sync.Mutex
	timeout       time.Duration
	log           *log.Logger
}

func NewRegistry(timeout time.Duration, log *log.Logger) *Registry {
	return &Registry{
		subscriptions: map[string]chan EventBlock{},
		timeout:       timeout,
		log:           log,
	}
}

// Subscribe is idempotent for a given subscription name
func (r *Registry) Subscribe(name string, c chan EventBlock) {
	r.m.Lock()
	defer r.m.Unlock()
	if _, ok := r.subscriptions[name]; !ok {
		r.subscriptions[name] = c
	}
	if r.log != nil {
		r.log.Println(`subscribed ` + name)
	}
}

func (r *Registry) Unsubscribe(name string) {
	r.m.Lock()
	defer r.m.Unlock()
	if _, ok := r.subscriptions[name]; ok {
		delete(r.subscriptions, name)
		if r.log != nil {
			r.log.Println(`unsubscribed ` + name)
		}
	}
}

// Publish is called by the goroutine that has gathered events - typically via
// the Write function. Publish is synchronous on a per-Registry basis however,
// the actual publication operation for each subscriber happens asynchronously
// in separate goroutines.
//
// In the event that a subscriber's channel is closed it will be automatically
// unsubscribed from the Registry.
//
// Publication to each subscriber will timeout after the duration specified
// during Registry configuration. In such an event the EventBlock will not be
// written to the subscriber and an error message will be written to the log.
// Automatically unsubscribing the subscriber in this case would cause more
// significant data loss in the event of a transient backpressure issue.
func (r *Registry) Publish(eb EventBlock) {
	r.m.Lock()
	defer r.m.Unlock()
	for k, v := range r.subscriptions {
		if r.log != nil {
			r.log.Println(`notifying ` + k)
		}
		go func(name string, sub chan EventBlock) {
			// if a reader closes a channel unsubscribe that reader
			defer func() {
				if rr := recover(); rr != nil {
					r.Unsubscribe(name)
				}
			}()
			select {
			case sub <- eb:
			case <-time.After(r.timeout):
				if r.log != nil {
					r.log.Println(`event publishing timeout for subscription:` + name)
				}
			}
		}(k, v)
	}
}

type Event interface {
	Name() string
	Value() float64
	Pair() Pair
}

type Pair struct {
	n string
	v float64
}

func (p *Pair) Name() string {
	return p.n
}
func (p *Pair) Value() float64 {
	return p.v
}
func (p *Pair) Pair() Pair {
	return *p
}

type Counter struct {
	n string
	v float64
}

func NewCounter(name string) *Counter {
	return &Counter{n: name}
}

func (c *Counter) Add(d float64) {
	c.v += d
}

func (c *Counter) Name() string {
	return c.n
}

func (c *Counter) Value() float64 {
	return c.v
}

func (c *Counter) Pair() Pair {
	return Pair{n: c.n, v: c.v}
}

type Timer struct {
	n string
	s time.Time
	e time.Time
}

func NewTimer(name string) *Timer {
	return &Timer{n: name}
}

func NewStartedTimer(name string) *Timer {
	t := NewTimer(name)
	t.Start()
	return t
}

func (t *Timer) Name() string {
	return t.n
}

func (t *Timer) Value() float64 {
	return t.e.Sub(t.s).Round(time.Microsecond).Seconds()
}

func (t *Timer) Pair() Pair {
	return Pair{n: t.n, v: t.Value()}
}

func (t *Timer) Start() *Timer {
	if t.s.IsZero() {
		t.s = time.Now()
	}
	return t
}

func (t *Timer) Stop() *Timer {
	if t.e.IsZero() {
		t.e = time.Now()
	}
	return t
}

type EventBlock struct {
	tags   map[string]string
	events []Pair
}

func Write(ctx context.Context, es ...Event) error {
	ec, ok := FromContext(ctx)
	if !ok {
		return fmt.Errorf(`context does not contain EventsConfig`)
	}
	eb := EventBlock{tags: ec.Tags}
	for _, e := range es {
		eb.events = append(eb.events, e.Pair())
	}
	ec.Registry.Publish(eb)
	return nil
}
