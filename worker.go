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
		// Throttle activity dispatch to 1/second at the server level, simulating
		// a kitchen that can only start 1 pizza per second regardless of queue depth.
		TaskQueueActivitiesPerSecond: 1,
	})
	w.RegisterWorkflow(PizzaOrderWorkflow)
	w.RegisterActivity(MakePizzaActivity)

	log.Println("Worker started. Polling task queue:", TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatal("Worker stopped:", err)
	}
}
