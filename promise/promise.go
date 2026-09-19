package promise

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var ErrTimeout = errors.New("timeout")

type Promise[T any] struct {
	pending bool

	executor func(resolve func(any), reject func(any))

	result any

	err error

	isHandlePanic bool

	mutex sync.Mutex

	elapseTime time.Duration

	calTime bool

	started bool

	wg sync.WaitGroup

	// Done channel support: lazily created, closed on resolve/reject.
	done      chan struct{}
	isSettled bool
}

func (this *Promise[T]) HandlePanic(v bool) *Promise[T] {
	this.isHandlePanic = v
	return this
}

type Timeout struct {
	mu        sync.Mutex
	closeChan chan struct{}
}

func (this *Timeout) IsClose() bool {
	if this == nil {
		return true
	}
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.closeChan == nil
}

// Close cancels the timeout. Racing the timer goroutine's teardown used to
// be both a data race (nil write vs read) and a send-on-closed panic; the
// mutex makes check-close-nil atomic with respect to teardown.
func (this *Timeout) Close() {
	if this == nil {
		return
	}
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closeChan != nil {
		select {
		case this.closeChan <- struct{}{}:
		default:
		}
	}
}

type IntervalWaitFinish struct {
	interval time.Duration
	stop     int32 // atomic: 0=running, 1=stopped
	done     chan struct{}
	lastTick time.Time
	mutex    sync.Mutex
}

func (this *IntervalWaitFinish) execute(duration time.Duration, f func(*IntervalWaitFinish, time.Time, time.Duration)) {
	this.done = make(chan struct{})
	go func() {
		this.lastTick = time.Now()
		for {
			if atomic.LoadInt32(&this.stop) != 0 {
				break
			}
			now := time.Now()
			delta := now.Sub(this.lastTick)
			if delta < duration {
				f(this, now, duration)
				sleepDur := duration - now.Sub(this.lastTick)
				if sleepDur > 0 {
					timer := time.NewTimer(sleepDur)
					select {
					case <-timer.C:
					case <-this.done:
						timer.Stop()
						break
					}
				}
			} else {
				f(this, now, delta)
			}
			this.lastTick = now
		}
	}()
}

func (this *IntervalWaitFinish) Close() {
	atomic.StoreInt32(&this.stop, 1)
	close(this.done)
}

type Interval struct {
	interval  time.Duration
	ticker    *time.Ticker
	mu        sync.Mutex
	closeChan chan struct{}
	f         func(interval *Interval)
}

func (this *Interval) IsClose() bool {
	if this == nil {
		return true
	}
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.closeChan == nil
}

// Close stops the interval; same check-close-nil atomicity as Timeout.Close.
func (this *Interval) Close() {
	if this == nil {
		return
	}
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.closeChan != nil {
		select {
		case this.closeChan <- struct{}{}:
		default:
		}
	}
}

func (this *Timeout) execute(duration time.Duration, f func(*Timeout)) {
	this.mu.Lock()
	this.closeChan = make(chan struct{})
	ch := this.closeChan
	this.mu.Unlock()
	go func() {
		select {
		case <-ch:
		case <-time.After(duration):
			f(this)
		}
		this.mu.Lock()
		close(this.closeChan)
		this.closeChan = nil
		this.mu.Unlock()
	}()
}

// Done returns a channel that is closed when the promise resolves or rejects.
// This enables use of Promise in select statements alongside other channels:
//
//	select {
//	case <-p.Done():
//	    result, err := p.Await()  // safe: already settled
//	case <-ctx.Done():
//	    // cancelled
//	}
func (this *Promise[T]) Done() <-chan struct{} {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if this.done == nil {
		this.done = make(chan struct{})
		if this.isSettled {
			close(this.done)
		}
	}
	return this.done
}

// closeDone closes the done channel if it has been created.
// Must be called with this.mutex held.
func (this *Promise[T]) closeDone() {
	this.isSettled = true
	if this.done != nil {
		close(this.done)
	}
}

// Any converts Promise[T] to Promise[any] safely.
func (this *Promise[T]) Any() *Promise[any] {
	return Async[any](func(resolve func(any), reject func(any)) {
		result, err := this.Await()
		if err != nil {
			reject(err)
			return
		}
		resolve(result)
	})
}

func (this *Promise[T]) CalTime() *Promise[T] {
	this.calTime = true
	return this
}

func (this *Promise[T]) Elapse() time.Duration {
	return this.elapseTime
}

func (this *Promise[T]) Resolve(resolution any) {
	this.mutex.Lock()

	if !this.pending {
		this.mutex.Unlock()
		return
	}

	switch result := resolution.(type) {
	case *Promise[T]:
		// Release mutex before blocking on inner Await to prevent deadlock.
		this.mutex.Unlock()
		flattenedResult, err := result.Await()
		if err != nil {
			this.Reject(err)
			return
		}
		this.mutex.Lock()
		if !this.pending {
			this.mutex.Unlock()
			return
		}
		this.result = flattenedResult
	default:
		this.result = result
	}

	this.pending = false
	this.closeDone()
	this.mutex.Unlock()
	this.wg.Done()
}

func (this *Promise[T]) Reject(err any) {
	this.mutex.Lock()

	if !this.pending {
		this.mutex.Unlock()
		return
	}

	if err1, ok := err.(error); ok {
		this.err = err1
	} else {
		this.err = fmt.Errorf("%v", err)
	}
	this.pending = false
	this.closeDone()
	this.mutex.Unlock()
	this.wg.Done()
}

func (this *Promise[T]) handlePanic() {
	var r = recover()
	if r != nil {
		if err, ok := r.(error); ok {
			this.Reject(err)
		} else {
			this.Reject(fmt.Errorf("panic: %v", r))
		}
	}
}

// Then chains a type-safe fulfillment handler that transforms Promise[T] into Promise[U].
func (this *Promise[T]) Then[U any](fulfillment func(T) U) *Promise[U] {
	return Async[U](func(resolve func(U), reject func(any)) {
		result, err := this.Await()
		if err != nil {
			reject(err)
			return
		}
		resolve(fulfillment(result))
	})
}

// ThenTyped chains a type-safe fulfillment handler that preserves the same type.
//
// Deprecated: Then is now a generic method covering this case; use Then instead.
func (this *Promise[T]) ThenTyped(fulfillment func(T) T) *Promise[T] {
	return this.Then(fulfillment)
}

// Catch handles rejection with type-safe recovery: the recovery value returned
// by the handler becomes the fulfillment value of the resulting promise.
func (this *Promise[T]) Catch(rejection func(err error) T) *Promise[T] {
	return Async[T](func(resolve func(T), reject func(any)) {
		result, err := this.Await()
		if err != nil {
			resolve(rejection(err))
			return
		}
		resolve(result)
	})
}

// CatchTyped handles rejection with type-safe recovery.
//
// Deprecated: Catch is now a generic method covering this case; use Catch instead.
func (this *Promise[T]) CatchTyped(rejection func(err error) T) *Promise[T] {
	return this.Catch(rejection)
}

// Map transforms Promise[T] to Promise[U].
func (this *Promise[T]) Map[U any](mapper func(T) U) *Promise[U] {
	return this.Then(mapper)
}

// FlatMap chains a handler returning another promise, flattening the nesting:
// a Promise[T] whose handler returns *Promise[U] yields Promise[U].
func (this *Promise[T]) FlatMap[U any](mapper func(T) *Promise[U]) *Promise[U] {
	return Async[U](func(resolve func(U), reject func(any)) {
		result, err := this.Await()
		if err != nil {
			reject(err)
			return
		}
		innerResult, innerErr := mapper(result).Await()
		if innerErr != nil {
			reject(innerErr)
			return
		}
		resolve(innerResult)
	})
}

func (this *Promise[T]) Await() (T, error) {
	this.Async()
	if this.calTime {
		start := time.Now()
		this.wg.Wait()
		this.elapseTime = time.Now().Sub(start)
		if this.result == nil {
			var ret T
			return ret, this.err
		}
		return this.result.(T), this.err
	}
	this.wg.Wait()
	if this.result == nil {
		var ret T
		return ret, this.err
	}
	return this.result.(T), this.err
}

// AwaitTimeOut waits for the promise to complete or times out.
// On timeout, the promise is rejected with ErrTimeout so all waiters are unblocked.
func (this *Promise[T]) AwaitTimeOut(t time.Duration) (T, error) {
	this.Async()
	wgChan := make(chan struct{})
	go func() {
		this.wg.Wait()
		close(wgChan)
	}()

	if this.calTime {
		start := time.Now()
		select {
		case <-time.After(t):
			this.elapseTime = time.Now().Sub(start)
			this.Reject(ErrTimeout) // unblock wg-watcher goroutine
			var ret T
			return ret, ErrTimeout
		case <-wgChan:
			this.elapseTime = time.Now().Sub(start)
			if this.result == nil {
				var ret T
				return ret, this.err
			}
			return this.result.(T), this.err
		}
	}

	select {
	case <-time.After(t):
		this.Reject(ErrTimeout) // unblock wg-watcher goroutine
		var ret T
		return ret, ErrTimeout
	case <-wgChan:
		if this.result == nil {
			var ret T
			return ret, this.err
		}
		return this.result.(T), this.err
	}
}

func (this *Promise[T]) AsCallback(f func(any, error)) {
	go func() {
		this.wg.Wait()
		f(this.result, this.err)
	}()
}

type resolutionHelper struct {
	index int
	data  any
}

func SetTimeout(duration time.Duration, f func(*Timeout)) *Timeout {
	ret := &Timeout{}
	ret.execute(duration, f)
	return ret
}

func SetIntervalWaitFinish(duration time.Duration, f func(*IntervalWaitFinish, time.Time, time.Duration)) *IntervalWaitFinish {
	ret := &IntervalWaitFinish{
		interval: duration,
	}
	ret.execute(duration, f)
	return ret
}

func SetInterval(duration time.Duration, f func(*Interval)) *Interval {
	ret := &Interval{
		interval:  duration,
		ticker:    time.NewTicker(duration),
		closeChan: make(chan struct{}),
		f:         f,
	}
	ret.mu.Lock()
	ch := ret.closeChan
	ret.mu.Unlock()
	go func() {
		defer ret.ticker.Stop()
	LOOP:
		for {
			select {
			case <-ret.ticker.C:
				f(ret)
			case <-ch:
				ret.mu.Lock()
				close(ret.closeChan)
				ret.closeChan = nil
				ret.mu.Unlock()
				break LOOP
			}
		}
	}()
	return ret
}

func Async[T any](executor func(resolve func(T), reject func(any))) *Promise[T] {
	var p = &Promise[T]{
		pending:       true,
		isHandlePanic: true,
		executor: func(resolve func(any), reject func(any)) {
			executor(func(t T) {
				resolve(t)
			}, reject)
		},
	}
	p.Async()
	return p
}

func Create[T any](executor func(resolve func(T), reject func(any))) *Promise[T] {
	return &Promise[T]{
		pending:       true,
		isHandlePanic: true,
		executor: func(resolve func(any), reject func(any)) {
			executor(func(t T) {
				resolve(t)
			}, reject)
		},
	}
}

func (this *Promise[T]) Async() {
	this.mutex.Lock()
	if this.started {
		this.mutex.Unlock()
		return
	}
	this.started = true
	this.wg.Add(1)
	this.mutex.Unlock()

	go func() {
		if this.isHandlePanic {
			defer this.handlePanic()
		}
		this.executor(this.Resolve, this.Reject)
	}()
}

func Sleep(duration time.Duration) *Promise[any] {
	return Async[any](func(resolve func(any), reject func(any)) {
		time.Sleep(duration)
		resolve(nil)
	})
}

// Await blocks until the promise settles and returns its typed result.
func Await[T any](p *Promise[T]) (T, error) {
	return p.Await()
}

// Each executes promises sequentially, resolving with a slice of all results.
// Use All for concurrent execution.
//
// Deprecated: Each is untyped; use EachTyped for a type-safe result slice.
func Each(promises ...*Promise[any]) *Promise[any] {
	return Create[any](func(resolve func(any), reject func(any)) {
		resolutions := make([]any, 0)
		for _, promise := range promises {
			result, err := promise.Await()
			if err != nil {
				reject(err)
				return
			}
			resolutions = append(resolutions, result)
		}
		resolve(resolutions)
	})
}

// Deprecated: All is untyped; use AllTyped for a type-safe result slice.
func All(promises ...*Promise[any]) *Promise[any] {
	psLen := len(promises)
	if psLen == 0 {
		return Resolve[any]([]any{})
	}

	return Create[any](func(resolve func(any), reject func(any)) {
		resolutionsChan := make(chan resolutionHelper, psLen)
		errorChan := make(chan error, psLen)

		for i, promise := range promises {
			i, promise := i, promise // capture loop variables
			promise.Then(func(data any) any {
				resolutionsChan <- resolutionHelper{i, data}
				return data
			}).Catch(func(err error) any {
				errorChan <- err
				return err
			}).Async()
		}

		resolutions := make([]any, psLen)
		for x := 0; x < psLen; x++ {
			select {
			case resolution := <-resolutionsChan:
				resolutions[resolution.index] = resolution.data
			case err := <-errorChan:
				reject(err)
				return
			}
		}
		resolve(resolutions)
	})
}

// Deprecated: Race is untyped; use RaceTyped to preserve the element type.
func Race(promises ...*Promise[any]) *Promise[any] {
	psLen := len(promises)
	if psLen == 0 {
		return Resolve[any](nil)
	}

	return Create[any](func(resolve func(any), reject func(any)) {
		resolutionsChan := make(chan any, psLen)
		errorChan := make(chan error, psLen)

		for _, promise := range promises {
			promise := promise // capture loop variable
			promise.Then(func(data any) any {
				resolutionsChan <- data
				return data
			}).Catch(func(err error) any {
				errorChan <- err
				return err
			}).Async()
		}

		select {
		case resolution := <-resolutionsChan:
			resolve(resolution)
		case err := <-errorChan:
			reject(err)
		}
	})
}

func AllSettled(promises ...*Promise[any]) *Promise[any] {
	psLen := len(promises)
	if psLen == 0 {
		return Resolve[any](nil)
	}

	return Create[any](func(resolve func(any), reject func(any)) {
		resolutionsChan := make(chan resolutionHelper, psLen)

		for i, promise := range promises {
			i, promise := i, promise // capture loop variables
			promise.Then(func(data any) any {
				resolutionsChan <- resolutionHelper{i, data}
				return data
			}).Catch(func(err error) any {
				resolutionsChan <- resolutionHelper{i, err}
				return err
			}).Async()
		}

		resolutions := make([]any, psLen)
		for x := 0; x < psLen; x++ {
			resolution := <-resolutionsChan
			resolutions[resolution.index] = resolution.data
		}
		resolve(resolutions)
	})
}

// EachTyped executes typed promises sequentially, resolving with []T in order.
// Use AllTyped for concurrent execution.
func EachTyped[T any](promises ...*Promise[T]) *Promise[[]T] {
	return Create[[]T](func(resolve func([]T), reject func(any)) {
		resolutions := make([]T, 0, len(promises))
		for _, promise := range promises {
			result, err := promise.Await()
			if err != nil {
				reject(err)
				return
			}
			resolutions = append(resolutions, result)
		}
		resolve(resolutions)
	})
}

// AllTyped runs typed promises concurrently, resolving with []T in input order
// or rejecting with the first error.
func AllTyped[T any](promises ...*Promise[T]) *Promise[[]T] {
	psLen := len(promises)
	if psLen == 0 {
		return Resolve[[]T]([]T{})
	}

	return Create[[]T](func(resolve func([]T), reject func(any)) {
		resolutionsChan := make(chan resolutionHelper, psLen)
		errorChan := make(chan error, psLen)

		for i, promise := range promises {
			i, promise := i, promise // capture loop variables
			promise.Then(func(data T) T {
				resolutionsChan <- resolutionHelper{i, data}
				return data
			}).Catch(func(err error) T {
				errorChan <- err
				var zero T
				return zero
			}).Async()
		}

		resolutions := make([]T, psLen)
		for x := 0; x < psLen; x++ {
			select {
			case resolution := <-resolutionsChan:
				resolutions[resolution.index] = resolution.data.(T)
			case err := <-errorChan:
				reject(err)
				return
			}
		}
		resolve(resolutions)
	})
}

// RaceTyped settles with the first settled promise, preserving the element type.
func RaceTyped[T any](promises ...*Promise[T]) *Promise[T] {
	psLen := len(promises)
	if psLen == 0 {
		var zero T
		return Resolve[T](zero)
	}

	return Create[T](func(resolve func(T), reject func(any)) {
		resolutionsChan := make(chan T, psLen)
		errorChan := make(chan error, psLen)

		for _, promise := range promises {
			promise := promise // capture loop variable
			promise.Then(func(data T) T {
				resolutionsChan <- data
				return data
			}).Catch(func(err error) T {
				errorChan <- err
				var zero T
				return zero
			}).Async()
		}

		select {
		case resolution := <-resolutionsChan:
			resolve(resolution)
		case err := <-errorChan:
			reject(err)
		}
	})
}

// Resolve creates an already-resolved promise with the given value.
func Resolve[T any](resolution T) *Promise[T] {
	return Async[T](func(resolve func(T), reject func(any)) {
		resolve(resolution)
	})
}

// Reject creates an already-rejected promise with the given error.
func Reject[T any](err error) *Promise[T] {
	return Async[T](func(resolve func(T), reject func(any)) {
		reject(err)
	})
}

// ============================================================================
// Type Conversion Functions
// ============================================================================

// From converts Promise[any] to Promise[T] with type assertion.
func From[T any](p *Promise[any]) *Promise[T] {
	return Async[T](func(resolve func(T), reject func(any)) {
		result, err := p.Await()
		if err != nil {
			reject(err)
			return
		}
		if typed, ok := result.(T); ok {
			resolve(typed)
		} else {
			reject(errors.New("type assertion failed: value is not of expected type"))
		}
	})
}

// ToAny converts Promise[T] to Promise[any].
func ToAny[T any](p *Promise[T]) *Promise[any] {
	return p.Any()
}

// ============================================================================
// Deprecated Package-Level Twins (compatibility shims for the generic methods)
// ============================================================================

// Then transforms a Promise[T] to Promise[U] with type-safe transformation.
//
// Deprecated: use the Then method instead: p.Then(fulfillment).
func Then[T, U any](p *Promise[T], fulfillment func(T) U) *Promise[U] {
	return Async[U](func(resolve func(U), reject func(any)) {
		result, err := p.Await()
		if err != nil {
			reject(err)
			return
		}
		resolve(fulfillment(result))
	})
}

// Catch handles errors with type-safe recovery.
//
// Deprecated: use the Catch method instead: p.Catch(rejection).
func Catch[T any](p *Promise[T], rejection func(error) T) *Promise[T] {
	return Async[T](func(resolve func(T), reject func(any)) {
		result, err := p.Await()
		if err != nil {
			resolve(rejection(err))
			return
		}
		resolve(result)
	})
}

// Map transforms Promise[T] to Promise[U] (alias for Then).
//
// Deprecated: use the Map method instead: p.Map(mapper).
func Map[T, U any](p *Promise[T], mapper func(T) U) *Promise[U] {
	return Then(p, mapper)
}

// FlatMap chains promises, flattening nested Promise[Promise[U]] to Promise[U].
//
// Deprecated: use the FlatMap method instead: p.FlatMap(mapper).
func FlatMap[T, U any](p *Promise[T], mapper func(T) *Promise[U]) *Promise[U] {
	return Async[U](func(resolve func(U), reject func(any)) {
		result, err := p.Await()
		if err != nil {
			reject(err)
			return
		}
		innerResult, innerErr := mapper(result).Await()
		if innerErr != nil {
			reject(innerErr)
			return
		}
		resolve(innerResult)
	})
}
