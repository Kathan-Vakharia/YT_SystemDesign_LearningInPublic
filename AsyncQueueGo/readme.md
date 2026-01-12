# Async Queue Demo - Restaurant Order System

## What This Demo Shows

This project simulates a restaurant kitchen using Go's built-in channels to demonstrate:
- **Async queues** (orders waiting to be cooked)
- **Consumer groups** (multiple chefs working together)
- **Retry logic** (what happens when an order fails)

## The Flow

1. **Producer (Waiter)**: Takes orders from customers and puts them in the queue
2. **Queue (Ticket Rail)**: Holds all pending orders
3. **Consumer Group (Chefs)**: Multiple workers pick up orders and "cook" them
4. **Retry Logic**: If cooking fails (random chance), the order is tried to process again [Local Retry]

## Key Concepts Explained

### Channels in Go
- Think of channels as pipes that carry data between goroutines
- `chan Order` is like the ticket rail holding orders
- Sending: `orderQueue <- order` (waiter clips ticket)
- Receiving: `order := <-orderQueue` (chef takes ticket)

### Goroutines
- Lightweight threads that run concurrently
- Each consumer (chef) runs in its own goroutine
- The `go` keyword starts a goroutine: `go consumer.Start()`

### WaitGroups
- A way to wait for multiple goroutines to finish
- Like a manager counting chefs before closing the kitchen
- `wg.Add(1)`: expect one more goroutine
- `wg.Done()`: one goroutine finished
- `wg.Wait()`: wait for all to finish

## Running the Demo
```bash
go run main.go
```

## Expected Output

You'll see:
- Orders being placed in the queue
- Multiple chefs processing orders concurrently
- Some orders failing and being retried
- Final statistics

## Customization

In `main.go`, adjust:
- `numConsumers`: Number of chefs (workers)
- `numOrders`: Total orders to process
- `maxRetries`: How many times to retry failed orders

## Real-World Applications

Replace "cooking orders" with:
- Sending emails
- Processing images
- Generating reports
- Handling API requests