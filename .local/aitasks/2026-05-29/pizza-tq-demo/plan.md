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

## Phase 5: Priority Demo

The goal: flood the queue with 10 low-priority (priority 5) orders, wait a few seconds so the queue builds up, then submit 1 high-priority (priority 1) order. With the 1 RPS rate limit active, the high-priority order jumps ahead of all remaining low-priority orders in the queue.

`temporal.Priority{PriorityKey: N}` is set on the **activity options** inside the workflow, so the server uses it when dispatching activity tasks from the queue. Range is 1-5 (1=highest, 5=lowest, default=3). The `temporal` package re-exports `internal.Priority`, so import `go.temporal.io/sdk/temporal` and use `PriorityKey int`.

### Task 5a: Add PriorityKey to PizzaOrder and wire it through the workflow

**Files:**
- Modify: `pizza.go`

- [ ] **Step 1: Add `PriorityKey int` to `PizzaOrder` and pass it to activity options**

Updated `pizza.go`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "pizza-orders"

type PizzaOrder struct {
	OrderID     string
	CustomerID  string
	Item        string
	PriorityKey int // 1=highest priority, 5=lowest; 0 means use server default (3)
}

func PizzaOrderWorkflow(ctx workflow.Context, order PizzaOrder) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
		Priority:            temporal.Priority{PriorityKey: order.PriorityKey},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	return workflow.ExecuteActivity(ctx, MakePizzaActivity, order).Get(ctx, nil)
}

func MakePizzaActivity(ctx context.Context, order PizzaOrder) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Making pizza", "orderID", order.OrderID, "customer", order.CustomerID)

	fmt.Printf("[%s] Making pizza  -- Order %-8s | Customer %-12s | %s\n",
		time.Now().Format("15:04:05"), order.OrderID, order.CustomerID, order.Item)

	//time.Sleep(3 * time.Second)

	fmt.Printf("[%s] Pizza ready   -- Order %-8s | Customer %-12s\n",
		time.Now().Format("15:04:05"), order.OrderID, order.CustomerID)
	logger.Info("Pizza ready", "orderID", order.OrderID)
	return nil
}
```

---

### Task 5b: Add runPriorityDemo to demo.go

**Files:**
- Modify: `demo.go`

- [ ] **Step 1: Append `runPriorityDemo` to demo.go**

Add after the existing `runRateLimitDemo` function:

```go
// runPriorityDemo floods the queue with 10 low-priority (priority 5) orders, waits
// a few seconds for them to back up, then submits a single high-priority (priority 1)
// order. With the 1 RPS rate limit active, the high-priority order should jump ahead
// of the remaining low-priority orders in the queue.
func runPriorityDemo() {
	c := newTemporalClient()
	defer c.Close()

	fmt.Println("=== Priority Demo ===")
	fmt.Println("Step 1: Flooding queue with 10 low-priority (priority 5) orders...")
	fmt.Println()
	for i := 1; i <= 10; i++ {
		submitOrder(c, PizzaOrder{
			OrderID:     fmt.Sprintf("prio-%02d", i),
			CustomerID:  fmt.Sprintf("customer-%02d", i),
			Item:        "Margherita",
			PriorityKey: 5,
		})
	}

	fmt.Println()
	fmt.Println("Waiting 3 seconds for the queue to build up...")
	time.Sleep(3 * time.Second)

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
```

Note: `time` is already imported via `runRateLimitDemo`'s package imports — confirm the import block includes it.

---

### Task 5c: Add `demo priority` subcommand to main.go

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Add `priority` case inside the `demo` switch and update usage string**

In the `demo` switch, add:
```go
case "priority":
    runPriorityDemo()
```

Updated usage string:
```
  go run . demo ratelimit   Flood the queue with 10 orders; watch 1/second drain
  go run . demo priority    Flood with low-priority orders, then jump the queue with a VIP order
```

---

### Task 5d: Build and verify

- [ ] **Step 1: Build**

```bash
go build ./...
```

Expected: clean, no errors.

---

## Phase 5 Verification

- [ ] **Step 1: Start worker (or restart if already running)**

```bash
go run . worker
```

The worker must be restarted to apply `TaskQueueActivitiesPerSecond: 1` if it was changed.

- [ ] **Step 2: Run the priority demo**

```bash
go run . demo priority
```

- [ ] **Step 3: Observe worker terminal**

Expected behavior:
- Seconds 0-3: low-priority orders start at 1/second (`customer-01`, `customer-02`, `customer-03`)
- At ~3s: VIP order is submitted
- Next order to start: `vip-customer` (Truffle Pizza) — jumps ahead of `customer-04` through `customer-10`
- After the VIP, remaining low-priority orders resume at 1/second

---

## Phase 6: Fairness Demo

The goal: flood the queue with 50 orders from Customer Alice, wait 5 seconds so the backlog builds, then submit 1 order from Customer Bob. With fairness enabled, the server puts Alice's orders in one virtual queue and Bob's in another and round-robins between them. Bob's single order should be dispatched within 1-2 seconds — not after all of Alice's remaining ~45 orders.

`FairnessKey` is a field on `temporal.Priority`, set alongside `PriorityKey` in the activity options inside the workflow. All fairness demo orders use default priority (0 → server default 3) so only the fairness behavior is isolated.

**Why it works:** The server creates one virtual queue per distinct `FairnessKey` within a priority tier and dispatches across them proportionally to weight. Both "alice" and "bob" default to weight 1.0, so each gets ~50% of dispatch slots. With 1 alice task dispatched per slot and 1 bob task total, Bob finishes in 1 slot rather than ~45.

The same `matching.useNewMatcher=true` server flag required for priority also enables fairness.

---

### Task 6a: Add FairnessKey to PizzaOrder and wire it through the workflow

**Files:**
- Modify: `pizza.go`

- [ ] **Step 1: Add `FairnessKey string` to `PizzaOrder` and include it in activity `Priority`**

Full updated `pizza.go`:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "pizza-orders"

type PizzaOrder struct {
	OrderID     string
	CustomerID  string
	Item        string
	PriorityKey int    // 1=highest priority, 5=lowest; 0 uses server default (3)
	FairnessKey string // groups orders into a virtual queue for fair dispatch; empty = no grouping
}

func PizzaOrderWorkflow(ctx workflow.Context, order PizzaOrder) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
		Priority: temporal.Priority{
			PriorityKey: order.PriorityKey,
			FairnessKey: order.FairnessKey,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	return workflow.ExecuteActivity(ctx, MakePizzaActivity, order).Get(ctx, nil)
}

func MakePizzaActivity(ctx context.Context, order PizzaOrder) error {
	logger := activity.GetLogger(ctx)
	logger.Info("Making pizza", "orderID", order.OrderID, "customer", order.CustomerID)

	fmt.Printf("[%s] Making pizza  -- Order %-8s | Customer %-12s | %s\n",
		time.Now().Format("15:04:05"), order.OrderID, order.CustomerID, order.Item)

	//time.Sleep(3 * time.Second)

	fmt.Printf("[%s] Pizza ready   -- Order %-8s | Customer %-12s\n",
		time.Now().Format("15:04:05"), order.OrderID, order.CustomerID)
	logger.Info("Pizza ready", "orderID", order.OrderID)
	return nil
}
```

Note: existing rate limit and priority demo orders have `FairnessKey: ""` which stays as the default — no behavior change for them. The `convertToPBPriority` SDK function only sends non-nil priority when at least one field is non-default, so empty `FairnessKey` + `PriorityKey 0` still sends nil (server defaults) for the rate limit demo.

---

### Task 6b: Add runFairnessDemo to demo.go

**Files:**
- Modify: `demo.go`

- [ ] **Step 1: Append `runFairnessDemo` to the end of demo.go**

```go
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
	fmt.Println("not after all ~45 remaining Alice orders (which would take ~45 more seconds).")
}
```

---

### Task 6c: Add `demo fairness` subcommand to main.go

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Add `fairness` case to the demo switch**

In the `demo` switch block, add after the `priority` case:
```go
case "fairness":
    runFairnessDemo()
```

- [ ] **Step 2: Update the usage string**

Add to the usage string:
```
  go run . demo fairness    Flood with Alice's orders, then show Bob jumping the queue
```

---

### Task 6d: Build and verify

- [ ] **Step 1: Build**

```bash
go build ./...
```

Expected: clean, no errors.

---

## Phase 6 Verification

Requires the server running with `matching.useNewMatcher=true` (same as priority demo):

```bash
temporal server start-dev \
  --dynamic-config-value "matching.useNewMatcher=true" \
  --dynamic-config-value "matching.enableFairness=true"
```

- [ ] **Step 1: Start the worker**

```bash
go run . worker
```

- [ ] **Step 2: Run the fairness demo**

```bash
go run . demo fairness
```

- [ ] **Step 3: Observe worker terminal**

Expected behavior:
- Seconds 0-5: Alice's orders start at 1/second (`alice` logs appear)
- At ~5s: Bob's order is submitted
- Within 1-2 seconds: `bob` appears in the worker logs interleaved with `alice`
- Bob's single order finishes; Alice's remaining ~45 orders continue at 1/second

Without fairness (for comparison): Bob would appear in the logs only after all ~45 remaining Alice orders complete (~45 more seconds).

---

## Troubleshooting

**Worker already running with old rate limit:** `TaskQueueActivitiesPerSecond` takes effect when the worker starts. Restart the worker after changing it.

**"already started" workflow errors on re-run:** Terminate old workflows first:
```bash
temporal workflow terminate --query 'WorkflowType="PizzaOrderWorkflow"' --reason "cleanup" --namespace default
```
