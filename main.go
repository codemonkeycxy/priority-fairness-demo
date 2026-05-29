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

// submitTestOrder submits one pizza order synchronously and prints the result.
// Use this to verify the end-to-end setup is working before running the full demos.
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
