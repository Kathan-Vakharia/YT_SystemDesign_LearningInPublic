# Autoscaling Worker Pool in Go

A demonstration of dynamic worker pool scaling based on queue depth, implementing a simple autoscaler that monitors workload and adjusts worker count in real-time.

## Overview

This implementation showcases a **channel-based autoscaling system** where workers are dynamically spawned based on queue pressure. The system starts with a single worker and scales up to a maximum of 5 workers when the job queue exceeds a defined threshold.

## Core Concepts

### 1. **Buffered Channels as Queues**
```go
jobs := make(chan Job, 100)
```
The buffered channel acts as a bounded queue that can hold up to 100 jobs. This provides:
- **Backpressure**: Producers block when the queue is full
- **Observable depth**: `len(jobs)` reveals current load
- **Decoupling**: Producers and consumers operate independently

### 2. **Goroutines as Workers**
Each worker runs as a lightweight goroutine, consuming from the shared `jobs` channel:
```go
for job := range jobs {
    process(id, job)
}
```
The `range` loop automatically terminates when the channel is closed.

### 3. **Synchronization with WaitGroup**
```go
var wg sync.WaitGroup
wg.Add(1)    // Before starting work
defer wg.Done()  // When work completes
wg.Wait()    // Block until all Done() calls match Add() calls
```
This ensures the main goroutine waits for all workers to finish before exiting.

## Architecture

```
┌─────────────┐
│   Producer  │
│  (main)     │
└──────┬──────┘
       │ sends jobs
       ▼
┌─────────────────────┐
│  jobs channel       │
│  (buffered, cap=100)│
└──────┬──────────────┘
       │ consumed by
       ▼
┌─────────────────────┐      ┌──────────────┐
│  Worker Pool        │◄─────┤  Autoscaler  │
│  (1-5 goroutines)   │      │  (monitor)   │
└─────────────────────┘      └──────────────┘
       │ processes              monitors len(jobs)
       ▼                        scales up when > 10
   [Job Output]
```

## Scaling Logic

### Trigger Conditions
```go
if queueSize > ScaleThreshold && workerCount < MaxWorkers {
    workerCount++
    startWorker(workerCount, jobs)
}
```

**When it scales UP:**
- Queue depth > 10 jobs (ScaleThreshold)
- Current workers < 5 (MaxWorkers)
- Check interval: every 200ms

**Mathematical reasoning:**
- Each job takes ~500ms to process
- One worker processes: 1000ms / 500ms = 2 jobs/second
- If queue grows > 10 jobs, we're accumulating backlog
- Adding workers increases throughput linearly (up to MaxWorkers)

### Why These Numbers?

| Parameter | Value | Reasoning |
|-----------|-------|-----------|
| `MaxWorkers` | 5 | Prevents resource exhaustion; reasonable for demo |
| `ScaleThreshold` | 10 | ~5 seconds of work for 1 worker; clear pressure signal |
| Ticker interval | 200ms | Fast enough to respond, not so fast to thrash |
| Job duration | 500ms | Simulates moderate I/O or computation |

## Code Flow

1. **Initialization** (main)
   - Create buffered `jobs` channel
   - Launch autoscaler goroutine
   - Start initial worker (worker 1)

2. **Job Production**
   - Send 50 jobs with random delays (0-50ms)
   - Simulates bursty traffic patterns

3. **Autoscaling Loop**
   - Every 200ms, check `len(jobs)`
   - If `len(jobs) > 10` AND `workerCount < 5`: spawn new worker
   - Increment `workerCount` to track total workers

4. **Worker Lifecycle**
   ```go
   wg.Add(1)           // Register with WaitGroup
   go func() {
       defer wg.Done() // Unregister on exit
       for job := range jobs {
           process(id, job)
       }
   }()
   ```

5. **Graceful Shutdown**
   ```go
   close(jobs)  // Signal "no more jobs"
   wg.Wait()    // Wait for all workers to drain
   ```

## Known Issues

### 🐛 Bug: Autoscaler Doesn't Gracefully Shutdown

**Problem:**  
The autoscaler goroutine runs indefinitely with `for range ticker.C`. When `main()` closes the `jobs` channel, the autoscaler keeps ticking, potentially trying to start workers after the channel is closed (though workers exit immediately).

**Why it matters:**
- Goroutine leak (in long-running apps)
- Race condition: autoscaler might read `len(jobs)` on a closed channel
- Unclear lifecycle management

### 🔧 PossibleSolution

#### Request–acknowledge pattern
```go
func main() {
    // ... setup jobs ...

    // 1. Create a channel for signaling stop
    quit := make(chan bool)
    // 2. Create a channel for the acknowledgement
    done := make(chan bool) 

    go autoscaler(jobs, quit, done)

    // ... traffic simulation ...
    
    // Cleanup Phase:
    close(jobs) 
    wg.Wait() // Wait for all WORKERS to finish
    
    // Now shut down the MANAGER (Autoscaler)
    fmt.Println("Signal sent: Stop Autoscaler")
    quit <- true 
    
    // BLOCK here until Autoscaler says it's actually finished
    <-done 
    fmt.Println("Main: Confirmed Autoscaler is stopped. Exiting.")
}

func autoscaler(jobs chan Job, quit chan bool, done chan bool) {
    ticker := time.NewTicker(200 * time.Millisecond)
    defer ticker.Stop() 

    for {
        select {
        case <-ticker.C:
            // ... scaling logic ...
        case <-quit:
            fmt.Println("🛑 Autoscaler received stop signal...")
            // Do any cleanup here (logging, closing DB connections, etc)
            
            done <- true // Send the "I am done" ACK back to main
            return 
        }
    }
}
```

## Example Output

```
🔧 Worker 1 started
--- Sending 50 jobs ---
Worker 1 processing Job 1
Worker 1 processing Job 2
⚠️  Load High (Queue: 15). Scaling UP! Starting Worker 2
🔧 Worker 2 started
Worker 2 processing Job 3
Worker 1 processing Job 4
⚠️  Load High (Queue: 22). Scaling UP! Starting Worker 3
🔧 Worker 3 started
...
zzz Worker 1 stopping
zzz Worker 2 stopping
--- All jobs processed ---
```

## Key Takeaways

1. **Buffered channels** provide built-in queue semantics with observable depth
2. **len(channel)** enables reactive scaling decisions (but beware race conditions in production)
3. **WaitGroup** ensures clean shutdown by tracking active goroutines
4. **Ticker** provides periodic monitoring without blocking the main logic
5. **Channel closure** propagates termination signals to all consumers automatically



## Running the Code

```bash
go run main.go
```

Watch the console output to see workers scaling up as the queue grows!