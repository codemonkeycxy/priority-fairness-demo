package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
)

func newTemporalClient() client.Client {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatal("Unable to connect to Temporal:", err)
	}
	return c
}

func submitOrder(c client.Client, order PizzaOrder) {
	opts := client.StartWorkflowOptions{
		ID:        "pizza-" + order.OrderID,
		TaskQueue: TaskQueue,
	}
	_, err := c.ExecuteWorkflow(context.Background(), opts, PizzaOrderWorkflow, order)
	if err != nil {
		log.Printf("Failed to submit order %s: %v", order.OrderID, err)
		return
	}
	fmt.Printf("  Submitted order %-10s for customer %s\n", order.OrderID, order.CustomerID)
}

// runPriorityDemo floods the queue with 20 low-priority (priority 5) orders, waits
// a few seconds for them to back up, then submits a single high-priority (priority 1)
// order. With the 1 RPS rate limit active, the high-priority order jumps ahead of all
// remaining low-priority orders in the queue.
func runPriorityDemo() {
	c := newTemporalClient()
	defer c.Close()

	fmt.Println("=== Priority Demo ===")
	fmt.Println("Step 1: Flooding queue with 20 low-priority (priority 5) orders...")
	fmt.Println()
	for i := 1; i <= 50; i++ {
		submitOrder(c, PizzaOrder{
			OrderID:     fmt.Sprintf("prio-%02d", i),
			CustomerID:  fmt.Sprintf("customer-%02d", i),
			Item:        "Margherita",
			PriorityKey: 5,
		})
	}

	fmt.Println()
	fmt.Println("Waiting 3 seconds for the queue to build up...")
	time.Sleep(5 * time.Second)

	fmt.Println()
	fmt.Println("Step 2: Submitting 1 high-priority (priority 1) VIP order...")
	submitOrder(c, PizzaOrder{
		OrderID:     "prio-vip",
		CustomerID:  "vip-customer",
		Item:        "Truffle Pizza",
		PriorityKey: 1,
	})
	fmt.Println()
	fmt.Println("VIP order submitted. Switch to the worker terminal.")
	fmt.Println("The VIP order should start next, jumping ahead of the remaining low-priority orders.")
}

// runFairnessDemo floods the queue with 50 orders from Customer Alice, waits 5 seconds
// for the backlog to build (~45 orders still pending), then submits 1 order from Customer
// Bob. With matching.useNewMatcher=true on the server, Alice and Bob each have their own
// virtual queue and get equal dispatch slots. Bob's single order should start within
// 1-2 seconds — not after all of Alice's remaining orders.
func runFairnessDemo() {
	c := newTemporalClient()
	defer c.Close()

	fmt.Println("=== Fairness Demo ===")
	fmt.Println("Step 1: Flooding queue with 50 orders from Customer Alice...")
	fmt.Println()
	for i := 1; i <= 50; i++ {
		submitOrder(c, PizzaOrder{
			OrderID:     fmt.Sprintf("fair-alice-%02d", i),
			CustomerID:  "alice",
			Item:        "Margherita",
			FairnessKey: "alice",
		})
	}

	fmt.Println()
	fmt.Println("Waiting 5 seconds for the queue to build up...")
	time.Sleep(5 * time.Second)

	fmt.Println()
	fmt.Println("Step 2: Submitting 1 order from Customer Bob...")
	submitOrder(c, PizzaOrder{
		OrderID:     "fair-bob-01",
		CustomerID:  "bob",
		Item:        "Pepperoni",
		FairnessKey: "bob",
	})
	fmt.Println()
	fmt.Println("Bob's order submitted. Switch to the worker terminal.")
	fmt.Println("Bob's order should start within 1-2 seconds (fairness round-robin),")
	fmt.Println("not after all ~45 remaining Alice orders (~45 more seconds without fairness).")
}

// runRateLimitDemo submits 10 pizza orders at once.
// With TaskQueueActivitiesPerSecond: 1 on the worker, the server throttles activity
// dispatch to 1/second — the worker starts exactly 1 pizza per second even though
// all orders arrived simultaneously.
func runRateLimitDemo() {
	c := newTemporalClient()
	defer c.Close()

	fmt.Println("=== Rate Limit Demo ===")
	fmt.Println("Submitting 10 pizza orders all at once...")
	fmt.Println()
	for i := 1; i <= 10; i++ {
		submitOrder(c, PizzaOrder{
			OrderID:    fmt.Sprintf("rl-%02d", i),
			CustomerID: fmt.Sprintf("customer-%02d", i),
			Item:       "Margherita",
		})
	}
	fmt.Println()
	fmt.Println("All 10 orders submitted. Switch to the worker terminal.")
	fmt.Println("You should see 1 new 'Making pizza' log per second — the kitchen is")
	fmt.Println("rate-limited to 1 order/second even though all 10 arrived at once.")
}
