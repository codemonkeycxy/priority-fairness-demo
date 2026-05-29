package main

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "pizza-orders"

type PizzaOrder struct {
	OrderID    string
	CustomerID string
	Item       string
}

func PizzaOrderWorkflow(ctx workflow.Context, order PizzaOrder) error {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
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
