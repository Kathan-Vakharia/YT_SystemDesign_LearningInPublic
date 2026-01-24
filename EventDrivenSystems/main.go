// 🎯 Event-Driven System Demo
// This program demonstrates how different parts of an application can communicate
// through events without being directly connected to each other (loose coupling)

package main

import (
	"fmt"
	"sync" // 🔒 For thread-safe operations (protecting shared data)
	"time" // ⏰ For timestamps and delays
)

// 📦 Event Structure
// Think of an Event like a message that gets passed around in your system
// Example: When someone places an order, we create an "OrderPlaced" event
type Event struct {
	Type      string    // 🏷️ Identifier (e.g., "OrderPlaced", "UserRegistered")
	Payload   any       // 📄 Flexible data (can be struct, string, int, anything!)
	Timestamp time.Time // ⏰ When it happened (helps with debugging and logging)
}

// 🚌 EventBus Structure
// The EventBus is like a central message hub (think of it as a post office)
// It keeps track of who wants to receive which types of events
type EventBus struct {
	subscribers map[string][]chan Event // 📬 Map of EventType -> List of Channels
	// Example: "OrderPlaced" -> [emailChannel, analyticsChannel]
	mu sync.RWMutex // 🔒 Thread safety lock (prevents race conditions)
	// RWMutex = Read-Write Mutex (allows multiple readers OR one writer)
}

// 🏗️ NewEventBus creates a new EventBus instance
// This is a constructor function - it initializes and returns a new EventBus
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[string][]chan Event), // 📝 Initialize empty map of subscribers
	}
}

// 📝 Subscribe method - Register to receive events of a specific type
// When you subscribe, you get a channel that will receive events
// Returns: A receive-only channel (<-chan) that will get events
func (b *EventBus) Subscribe(eventType string) <-chan Event {
	ch := make(chan Event, 10) // 📮 Buffered channel (can hold up to 10 events)
	// Buffering prevents blocking if consumer is slow

	b.mu.Lock()                                                     // 🔒 Lock for writing (exclusive access)
	b.subscribers[eventType] = append(b.subscribers[eventType], ch) // Add channel to subscriber list
	b.mu.Unlock()                                                   // 🔓 Release the lock

	return ch // 📬 Return the channel so subscriber can listen for events
}

// 📢 Publish method - Send an event to all subscribers
// This is how you broadcast an event to everyone who's interested
func (b *EventBus) Publish(event Event) {
	b.mu.RLock()                      // 🔒 Read lock (multiple readers can access simultaneously)
	subs := b.subscribers[event.Type] // Get all subscribers for this event type
	b.mu.RUnlock()                    // 🔓 Release the read lock

	// 📤 Send event to all subscribers
	for _, ch := range subs {
		ch <- event // 💌 Send event through the channel (non-blocking due to buffer)
	}
}

// 📧 EmailService - Simulates sending emails when events occur
// This function runs continuously, waiting for events on its channel
func EmailService(events <-chan Event) {
	for e := range events { // 🔄 Loop forever, receiving events from the channel
		fmt.Println("📧 Email sent for:", e.Payload)
		// 💡 In a real app, this would call an email API like SendGrid or AWS SES
	}
}

// 📊 AnalyticsService - Simulates logging analytics when events occur
// This runs independently from EmailService (they don't know about each other!)
func AnalyticsService(events <-chan Event) {
	for e := range events { // 🔄 Loop forever, receiving events from the channel
		fmt.Println("📊 Analytics logged:", e.Payload)
		// 💡 In a real app, this would send data to Google Analytics, Mixpanel, etc.
	}
}

// 🚀 Main function - Entry point of the program
func main() {
	// 🏗️ Step 1: Create the event bus (our central message hub)
	bus := NewEventBus()

	// 📝 Step 2: Register Subscribers (who wants to listen to what?)
	// Both services want to know when an order is placed
	emailEvents := bus.Subscribe("OrderPlaced")     // 📧 Email service subscribes
	analyticsEvents := bus.Subscribe("OrderPlaced") // 📊 Analytics service subscribes

	// 🏃 Step 3: Start Consumers in Background (Goroutines)
	// "go" keyword runs these functions concurrently (in parallel)
	go EmailService(emailEvents)         // 📧 Start email service in background
	go AnalyticsService(analyticsEvents) // 📊 Start analytics service in background

	// 📦 Step 4: Create and Publish Event
	// Simulate an order being placed in the system
	order := Event{
		Type:      "OrderPlaced", // 🏷️ Event type
		Payload:   "OrderID=123", // 📄 Event data
		Timestamp: time.Now(),    // ⏰ Current time
	}
	order2 := Event{
		Type:      "OrderPlaced", // 🏷️ Event type
		Payload:   "OrderID=124", // 📄 Event data
		Timestamp: time.Now(),    // ⏰ Current time
	}
	bus.Publish(order) // 📢 Broadcast the event to all subscribers!
	bus.Publish(order2) // 📢 Broadcast the event to all subscribers!

	// ⏳ Step 5: Wait for goroutines to process events
	// Without this, the program would exit before services can print
	time.Sleep(time.Second) // 💤 Sleep for 1 second

	// 💡 In a real application, you'd use sync.WaitGroup or context for proper shutdown
}
