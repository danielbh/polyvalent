// Worker will send 200 once job has been picked up

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

var (
	// A buffered channel that we can send work requests on.
	JobQueue chan Job
	// max length of handler payloads
	MaxRequestLength = int64(100)
	MaxWorker        = 10
	MaxQueue         = 1000
)

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
	maxWorkers int
}

type Payload struct {
	Message string `json:"message"`
}

type RequestBody struct {
	Payloads []Payload `json:"payloads"`
}

// Job represents the job to be run
type Job struct {
	Payload Payload
}

func (p *Payload) Run() error {
	fmt.Println("Job ran with message:", p.Message)
	return nil
}

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{WorkerPool: pool, maxWorkers: maxWorkers}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}
	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	fmt.Println("Dispatcher started")
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			fmt.Println("Job request received")
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	fmt.Println("New worker listening for jobs")
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				if err := job.Payload.Run(); err != nil {
					fmt.Println("Error running job: %s", err.Error())
				} else {

				}

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		fmt.Println("Worker stopping")
		w.quit <- true
	}()
}

func main() {
	JobQueue = make(chan Job, MaxQueue)
	dispatcher := NewDispatcher(MaxWorker)
	dispatcher.Run()
	http.HandleFunc("/", handler)
	http.ListenAndServe(":8080", nil)
}

func handler(w http.ResponseWriter, r *http.Request) {

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Read the body into a string for json decoding
	var content = &RequestBody{}
	err := json.NewDecoder(io.LimitReader(r.Body, MaxRequestLength)).Decode(&content)
	if err != nil {
		fmt.Println(err)
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Go through each payload and queue items individually to be posted to S3
	for _, payload := range content.Payloads {
		// let's create a job with the payload
		work := Job{Payload: payload}
		// fmt.Println("Job received via handler:", work)
		// Push the work onto the queue.
		JobQueue <- work
	}

	w.WriteHeader(http.StatusOK)
}
