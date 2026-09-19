package promise

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestAsyncRaceCondition tests the race condition in Async()
func TestAsyncRaceCondition(t *testing.T) {
	p := Create[int](func(resolve func(int), reject func(any)) {
		time.Sleep(50 * time.Millisecond)
		resolve(42)
	})

	// 启动多个 goroutine 同时调用 Await
	var wg sync.WaitGroup
	results := make([]int, 10)
	errors := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			val, err := p.Await()
			results[idx] = val
			errors[idx] = err
		}(i)
	}

	// 等待所有 goroutine 完成（如果有 bug，这里会永久阻塞）
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 验证所有结果都是 42
		for i, val := range results {
			if val != 42 {
				t.Errorf("goroutine %d got %d, expected 42", i, val)
			}
			if errors[i] != nil {
				t.Errorf("goroutine %d got error: %v", i, errors[i])
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out - likely deadlock due to race condition")
	}
}

// TestTimeoutClosePanic tests panic when closing timeout after it fires
func TestTimeoutClosePanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Close() panicked: %v", r)
		}
	}()

	timeout := SetTimeout(50*time.Millisecond, func(t *Timeout) {
		// Timeout fired
	})

	time.Sleep(100 * time.Millisecond)

	// 这应该不会 panic
	timeout.Close()
	timeout.Close() // 多次调用也不应该 panic
}

// TestIntervalClosePanic tests panic when closing interval multiple times
func TestIntervalClosePanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Close() panicked: %v", r)
		}
	}()

	count := 0
	interval := SetInterval(20*time.Millisecond, func(i *Interval) {
		count++
	})

	time.Sleep(100 * time.Millisecond)

	// 多次调用 Close 不应该 panic
	interval.Close()
	time.Sleep(10 * time.Millisecond)
	interval.Close()
}

// TestHandlePanicWithNonError tests panic handling with non-error/non-string values
func TestHandlePanicWithNonError(t *testing.T) {
	p := Async[int](func(resolve func(int), reject func(any)) {
		panic(123) // panic with int
	})

	_, err := p.Await()
	if err == nil {
		t.Fatal("expected error from panic")
	}
	// 当前实现会在这里 panic，因为 123 不是 string
}

// TestAwaitTimeoutGoroutineLeak tests goroutine leak in AwaitTimeOut
func TestAwaitTimeoutGoroutineLeak(t *testing.T) {
	// 创建一个永远不会完成的 promise
	p := Create[int](func(resolve func(int), reject func(any)) {
		// 永远不调用 resolve/reject
	})

	// 超时应该立即返回
	_, err := p.AwaitTimeOut(50 * time.Millisecond)
	if err != ErrTimeout {
		t.Errorf("expected timeout error, got %v", err)
	}

	// 注意：这里会有一个 goroutine 泄漏，它会永远等待 wg.Wait()
	// 这是当前实现的已知问题
}

// TestRaceUnusedPromises tests that unused promises in Race continue executing
func TestRaceUnusedPromises(t *testing.T) {
	executed := make([]bool, 3)
	var mu sync.Mutex

	promises := []*Promise[any]{
		Async[any](func(resolve func(any), reject func(any)) {
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			executed[0] = true
			mu.Unlock()
			resolve("first")
		}),
		Async[any](func(resolve func(any), reject func(any)) {
			time.Sleep(100 * time.Millisecond)
			mu.Lock()
			executed[1] = true
			mu.Unlock()
			resolve("second")
		}),
		Async[any](func(resolve func(any), reject func(any)) {
			time.Sleep(200 * time.Millisecond)
			mu.Lock()
			executed[2] = true
			mu.Unlock()
			resolve("third")
		}),
	}

	result := Race(promises...)
	val, err := result.Await()
	if err != nil {
		t.Fatalf("Race failed: %v", err)
	}
	if val != "first" {
		t.Errorf("expected 'first', got %v", val)
	}

	// 等待一段时间，看其他 promise 是否继续执行
	time.Sleep(250 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// 所有 promise 都应该执行完成（这是当前行为）
	for i, exec := range executed {
		if !exec {
			t.Logf("Promise %d did not execute (this is expected if we had cancellation)", i)
		}
	}
}

// TestMultipleResolve tests that multiple Resolve calls are ignored
func TestMultipleResolve(t *testing.T) {
	resolveChan := make(chan func(int), 1)
	p := Create[int](func(resolve func(int), reject func(any)) {
		resolveChan <- resolve
		// 等待外部调用 resolve
		time.Sleep(100 * time.Millisecond)
	})

	p.Async()

	// 获取 resolve 函数
	resolveFunc := <-resolveChan

	// 多次调用 resolve
	resolveFunc(1)
	resolveFunc(2)
	resolveFunc(3)

	val, err := p.Await()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 1 {
		t.Errorf("expected 1 (first resolve), got %d", val)
	}
}

// TestResolveAfterReject tests that resolve after reject is ignored
func TestResolveAfterReject(t *testing.T) {
	p := Async[int](func(resolve func(int), reject func(any)) {
		reject(errors.New("error"))
		resolve(42) // 应该被忽略
	})

	_, err := p.Await()
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "error" {
		t.Errorf("expected 'error', got %v", err)
	}
}

// BenchmarkThenChain benchmarks chained generic Then method calls
func BenchmarkThenChain(b *testing.B) {
	for i := 0; i < b.N; i++ {
		p := Resolve[int](1)
		result := p.Then(func(x int) int { return x + 1 }).
			Then(func(x int) int { return x + 1 }).
			Then(func(x int) int { return x + 1 }).
			Then(func(x int) int { return x + 1 }).
			Then(func(x int) int { return x + 1 })
		result.Await()
	}
}

// BenchmarkGenericThenChain benchmarks generic Then function
func BenchmarkGenericThenChain(b *testing.B) {
	for i := 0; i < b.N; i++ {
		p := Resolve[int](1)
		result := Then(
			Then(
				Then(
					Then(
						Then(p, func(x int) int { return x + 1 }),
						func(x int) int { return x + 1 },
					),
					func(x int) int { return x + 1 },
				),
				func(x int) int { return x + 1 },
			),
			func(x int) int { return x + 1 },
		)
		result.Await()
	}
}
