Status: in-progress

# Pizza Store Task Queue Demo Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable Temporal Go demo that illustrates task queue rate limiting, priority, and fairness using a pizza store analogy. Implemented incrementally — one feature at a time.

**Architecture:** A single `PizzaOrderWorkflow` workflow calls a `MakePizzaActivity` that sleeps 3 seconds (simulating kitchen work). The worker sets `TaskQueueActivitiesPerSecond` so the server throttles activity dispatch — this is the observable bottleneck for all three demo features.

**Tech Stack:** Go 1.26, Temporal Go SDK v1.44.1 (`go.temporal.io/sdk`), Temporal CLI (`temporal` binary), local Temporal dev server.

---

## File Map

| File | Purpose |
|------|---------|
| `go.mod` | Temporal SDK dependency (done) |
| `pizza.go` | `PizzaOrder` type, `PizzaOrderWorkflow`, `MakePizzaActivity`, `TaskQueue` constant |
| `worker.go` | Temporal worker setup — sets the activity rate limit |
| `demo.go` | Demo scenario functions (one per feature) |
| `main.go` | CLI entry: `worker`, `submit`, `demo <scenario>` |

All files are in `package main`.

---

## Prerequisites

1. Temporal dev server running: `temporal server start-dev`
2. `temporal` CLI on PATH
3. Go 1.21+

---

## ~~Phase 1: SDK Setup~~ (done)

`go.temporal.io/sdk v1.44.1` is already in `go.mod` and `go.sum`.

---

## ~~Phase 2: Core Workflow and Activity~~ (done)

`pizza.go` is written with `PizzaOrder`, `PizzaOrderWorkflow`, `MakePizzaActivity`.

Current `PizzaOrder` struct (simple — no priority/fairness fields yet):
```go
type PizzaOrder struct {
    OrderID    string
    CustomerID string
    Item       string
}
```

---

## ~~Phase 3: Worker and Basic CLI~~ (done)

`worker.go` and `main.go` are written. The worker starts and `go run . submit` sends a test order end-to-end.

---

## Phase 4: Rate Limit Demo

The goal: flood the pizza queue with 10 simultaneous orders. With `TaskQueueActivitiesPerSecond: 1`, the server throttles activity dispatch to 1/second — the worker logs show exactly 1 new pizza starting per second even though all 10 were submitted at once.

`TaskQueueActivitiesPerSecond` is a `worker.Options` field that sends the throttle limit to the **server**. The server enforces it across the entire task queue (not just per worker), making it the right knob for simulating a capacity-constrained kitchen.

### Task 4a: Update worker.go to set the activity rate limit

**Files:**
- Modify: `worker.go`

- [ ] **Step 1: Add `TaskQueueActivitiesPerSecond: 1` to worker options**

Replace the `worker.New(...)` call in `worker.go` with:

```go
w := worker.New(c, TaskQueue, worker.Options{
    // Throttle activity dispatch to 1/second at the server level.
    // This simulates a kitchen that can only start making 1 pizza per second,
    // regardless of how many orders arrive simultaneously.
    TaskQueueActivitiesPerSecond: 1,
})
```

Full updated `worker.go`:

```go
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func runWorker() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatal("Unable to connect to Temporal:", err)
	}
	defer c.Close()

	w := worker.New(c, TaskQueue, worker.Options{
		// Throttle activity dispatch to 1/second at the server level.
		// This simulates a kitchen that can only start making 1 pizza per second,
		// regardless of how many orders arrive simultaneously.
		TaskQueueActivitiesPerSecond: 1,
	})
	w.RegisterWorkflow(PizzaOrderWorkflow)
	w.RegisterActivity(MakePizzaActivity)

	log.Println("Worker started. Polling task queue:", TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal("Worker stopped:", err)
	}
}
```

---

### Task 4b: Create demo.go with the rate limit scenario

**Files:**
- Create: `demo.go`

- [ ] **Step 1: Create demo.go**

```go
package main

import (
	"context"
	"fmt"
	"log"

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

// runRateLimitDemo submits 10 pizza orders at once.
// With TaskQueueActivitiesPerSecond: 1 on the worker, the server throttles activity
// dispatch to 1/second — the worker will start exactly 1 pizza per second even
// though all orders arrived simultaneously.
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
	fmt.Println("You should see 1 new 'Making pizza' log every second — the kitchen")
	fmt.Println("is rate-limited to 1 order/second even though all 10 arrived at once.")
}
```

---

### Task 4c: Update main.go to add the demo subcommand

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Add `demo` subcommand handling**

Full updated `main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.temporal.io/sdk/client"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "worker":
		runWorker()
	case "submit":
		submitTestOrder()
	case "demo":
		if len(os.Args) < 3 {
			printUsage()
			os.Exit(1)
		}
		switch os.Args[2] {
		case "ratelimit":
			runRateLimitDemo()
		default:
			fmt.Fprintf(os.Stderr, "Unknown demo: %s\n\n", os.Args[2])
			printUsage()
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Pizza Store Task Queue Demo

Usage:
  go run . worker           Start the worker (leave running in a separate terminal)
  go run . submit           Submit a single test order and wait for completion
  go run . demo ratelimit   Flood the queue with 10 orders; watch 1/second drain`)
}

func submitTestOrder() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatal("Unable to connect to Temporal:", err)
	}
	defer c.Close()

	order := PizzaOrder{
		OrderID:    "test-001",
		CustomerID: "test-customer",
		Item:       "Margherita",
	}
	opts := client.StartWorkflowOptions{
		ID:        "pizza-" + order.OrderID,
		TaskQueue: TaskQueue,
	}
	we, err := c.ExecuteWorkflow(context.Background(), opts, PizzaOrderWorkflow, order)
	if err != nil {
		log.Fatal("Failed to start workflow:", err)
	}
	fmt.Printf("Order submitted (workflow ID: %s, run ID: %s)\n", we.GetID(), we.GetRunID())
	fmt.Println("Waiting for pizza to be ready...")
	if err := we.Get(context.Background(), nil); err != nil {
		log.Fatal("Order failed:", err)
	}
	fmt.Println("Order complete!")
}
```

---

### Task 4d: Build and verify

- [ ] **Step 1: Build**

```bash
go build ./...
```

Expected: clean, no errors.

---

## Phase 4 Verification

- [ ] **Step 1: Start Temporal dev server (if not running)**

```bash
temporal server start-dev
```

- [ ] **Step 2: Start the worker in Terminal 1**

```bash
go run . worker
```

Expected: `Worker started. Polling task queue: pizza-orders`

- [ ] **Step 3: Run the rate limit demo in Terminal 2**

```bash
go run . demo ratelimit
```

Expected: all 10 "Submitted order..." lines print almost instantly.

- [ ] **Step 4: Observe worker Terminal 1**

Expected: a new `Making pizza` line appears **exactly 1 second apart** even though all 10 orders were submitted at once. The output should look like:

```
[15:00:00] Making pizza  -- Order rl-01    | customer-01  | Margherita
[15:00:01] Making pizza  -- Order rl-02    | customer-02  | Margherita
[15:00:02] Making pizza  -- Order rl-03    | customer-03  | Margherita
...
```

---

## Future Phases (planned separately)

- **Phase 5: Priority demo** — add `PriorityKey` to `PizzaOrder`, set on activity options, add `demo priority` command
- **Phase 6: Fairness demo** — add `FairnessKey` to `PizzaOrder`, set on activity options, add `demo fairness` command

---

## Troubleshooting

**Worker already running with old rate limit:** `TaskQueueActivitiesPerSecond` takes effect when the worker starts. Restart the worker after changing it.

**"already started" workflow errors on re-run:** Terminate old workflows first:
```bash
temporal workflow terminate --query 'WorkflowType="PizzaOrderWorkflow"' --reason "cleanup" --namespace default
```
