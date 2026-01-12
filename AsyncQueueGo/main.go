package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Order represents a task in our queue (like a restaurant order)
type Order struct {
	ID          int       // Unique identifier
	Description string    // What needs to be done
	RetryCount  int       // How many times we've tried processing this
	CreatedAt   time.Time // When the order was placed
}

// Stats tracks system performance
type Stats struct {
	mu             sync.Mutex
	Processed      int
	Failed         int
	Retried        int
	TotalProcessed int
}

func (s *Stats) IncrementProcessed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Processed++
	s.TotalProcessed++
}

func (s *Stats) IncrementFailed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Failed++
}

func (s *Stats) IncrementRetried() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Retried++
}

func (s *Stats) Print() {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Printf("\n=== Final Statistics ===\n")
	fmt.Printf("Successfully Processed: %d\n", s.Processed)
	fmt.Printf("Failed (max retries): %d\n", s.Failed)
	fmt.Printf("Retried: %d\n", s.Retried)
	fmt.Printf("Total Attempts: %d\n", s.TotalProcessed)
}

// Consumer represents a worker (chef) that processes orders
type Consumer struct {
	ID         int
	OrderQueue chan Order // The shared queue (ticket rail)
	Stats      *Stats
	MaxRetries int
	wg         *sync.WaitGroup
}

// Start begins consuming orders from the queue
func (c *Consumer) Start() {
	defer c.wg.Done() // Tell WaitGroup we're done when this function exits

	fmt.Printf("👨‍🍳 Chef %d is ready to cook!\n", c.ID)

	// Keep taking orders until the queue is closed
	for order := range c.OrderQueue {
		c.processOrder(order)
	}

	fmt.Printf("👨‍🍳 Chef %d is clocking out.\n", c.ID)
}

// processOrder simulates processing a task with local retry logic
func (c *Consumer) processOrder(order Order) {
	// Retry loop - same chef keeps trying until success or max retries
	for {
		fmt.Printf("👨‍🍳 Chef %d picked up Order #%d: %s (Attempt %d)\n",
			c.ID, order.ID, order.Description, order.RetryCount+1)

		// Simulate work (cooking takes time)
		processingTime := time.Duration(100+rand.Intn(400)) * time.Millisecond
		time.Sleep(processingTime)

		// Simulate random failures (20% chance of burning the dish)
		if rand.Float32() < 0.2 {
			fmt.Printf("❌ Chef %d FAILED Order #%d (attempt %d/%d)\n",
				c.ID, order.ID, order.RetryCount+1, c.MaxRetries)

			// Should we retry?
			if order.RetryCount < c.MaxRetries {
				order.RetryCount++
				c.Stats.IncrementRetried()

				// Retry locally - same chef tries again
				fmt.Printf("🔄 Chef %d retrying Order #%d locally (retry %d/%d)\n",
					c.ID, order.ID, order.RetryCount, c.MaxRetries)
				continue // Loop back and try again
			} else {
				// Max retries exceeded - send to dead letter queue (or log it)
				c.Stats.IncrementFailed()
				fmt.Printf("💀 Order #%d permanently failed after %d attempts\n",
					order.ID, order.RetryCount+1)
				return // Exit the function
			}
		}

		// Success!
		duration := time.Since(order.CreatedAt)
		c.Stats.IncrementProcessed()
		fmt.Printf("✅ Chef %d completed Order #%d in %dms (total time: %dms)\n",
			c.ID, order.ID, processingTime.Milliseconds(), duration.Milliseconds())
		return // Exit the function after success
	}
}

func main() {
	fmt.Println("🍽️  Restaurant Kitchen Simulation - Async Queue Demo")
	fmt.Println(strings.Repeat("=", 60))

	// Configuration
	const (
		numConsumers = 3  // Number of chefs (workers)
		numOrders    = 10 // Number of orders to process
		maxRetries   = 2  // Maximum retry attempts per order
		queueSize    = 5  // Buffer size (how many orders can wait)
	)

	// Initialize the queue (buffered channel)
	// Buffered means it can hold messages even if no one is reading yet
	orderQueue := make(chan Order, queueSize)

	// Initialize statistics tracker
	stats := &Stats{}

	// WaitGroup to wait for all consumers to finish
	var wg sync.WaitGroup

	// Start consumer group (our team of chefs)
	for i := 1; i <= numConsumers; i++ {
		wg.Add(1)
		consumer := &Consumer{
			ID:         i,
			OrderQueue: orderQueue,
			Stats:      stats,
			MaxRetries: maxRetries,
			wg:         &wg,
		}
		go consumer.Start() // Start each chef in their own goroutine
	}

	// Give consumers a moment to start
	time.Sleep(100 * time.Millisecond)

	// Producer: Create and send orders to the queue
	fmt.Printf("\n📋 Waiter is taking %d orders...\n\n", numOrders)
	for i := 1; i <= numOrders; i++ {
		order := Order{
			ID:          i,
			Description: fmt.Sprintf("Dish #%d", i),
			RetryCount:  0,
			CreatedAt:   time.Now(),
		}

		orderQueue <- order // Send to queue
		fmt.Printf("📝 Order #%d placed in queue\n", i)

		// Simulate orders coming in at different times
		time.Sleep(time.Duration(50+rand.Intn(150)) * time.Millisecond)
	}

	// Close the queue - signals "no more orders coming"
	// This will cause the consumers' range loops to exit
	close(orderQueue)
	fmt.Println("\n🚪 Kitchen is closed - no more orders accepted")

	// Wait for all consumers to finish processing
	wg.Wait()

	// Print final statistics
	stats.Print()

	fmt.Println("\n✨ Demo complete!")
}
