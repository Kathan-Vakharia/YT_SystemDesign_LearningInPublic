package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Job represents the work to be done.
type Job struct {
	ID int
}

// Global WaitGroup to ensure we wait for all workers to finish before exiting.
var wg sync.WaitGroup

// Settings for our autoscaler
const (
	MaxWorkers    = 5   // Don't scale beyond this
	ScaleThreshold = 10 // If queue has > 10 items, add a worker
)

func main() {
	// 1. Create a buffered channel (the queue).
	// We can hold 100 jobs before the sender blocks.
	jobs := make(chan Job, 100)

	// 2. Start the Autoscaler in the background.
	// It monitors the 'jobs' channel and adds workers.
	go autoscaler(jobs)

	// 3. Start one initial worker so work begins immediately.
	startWorker(1, jobs)

	// 4. Simulate Traffic (The "Producer")
	// We send 50 jobs.
	fmt.Println("--- Sending 50 jobs ---")
	for i := 1; i <= 50; i++ {
		jobs <- Job{ID: i}
		
		// Random tiny pause to simulate real inconsistent traffic
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
	}

	// 5. Cleanup
	close(jobs) // Close the channel so workers know no more data is coming.
	wg.Wait()   // Block here until all workers verify they are done.
	fmt.Println("--- All jobs processed ---")
}

// autoscaler monitors the queue depth and launches new workers.
func autoscaler(jobs chan Job) {
	// We keep a local count of workers we've started.
	workerCount := 1

	// Check the queue status every 200 milliseconds.
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		// If the channel is closed, stop scaling.
		// (In a real app, you might check this differently).
		if len(jobs) == 0 && workerCount > 1 {
			// Optional: Logic to scale down could go here
		}

		// SCALING LOGIC:
		// If queue length > Threshold AND we haven't hit MaxWorkers...
		queueSize := len(jobs)
		if queueSize > ScaleThreshold && workerCount < MaxWorkers {
			workerCount++
			fmt.Printf("⚠️  Load High (Queue: %d). Scaling UP! Starting Worker %d\n", queueSize, workerCount)
			startWorker(workerCount, jobs)
		}
	}
}

// startWorker launches a goroutine and manages the WaitGroup.
func startWorker(id int, jobs <-chan Job) {
	wg.Add(1) // Tell the WaitGroup we are starting a unit of work
	
	go func() {
		defer wg.Done() // Tell the WaitGroup we are done when this function exits
		fmt.Printf("🔧 Worker %d started\n", id)
		
		// The standard worker loop
		for job := range jobs {
			process(id, job)
		}
		
		fmt.Printf("zzz Worker %d stopping\n", id)
	}()
}

// process simulates work that takes time (500ms).
func process(workerID int, job Job) {
	fmt.Printf("Worker %d processing Job %d\n", workerID, job.ID)
	time.Sleep(500 * time.Millisecond) // Simulate expensive calculation
}

//! Bug: Autoscaler doesn't gracefully shutdown
//* Solutions:
//1. A channel for signalling shutdown and channel for signalling completion 
