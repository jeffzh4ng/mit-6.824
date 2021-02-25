package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	numberOfReduceTasks int
	mapWorkQueue WorkQueue
	reduceWorkQueue WorkQueue
}

type WorkQueue struct {
	mu sync.Mutex
	queue map[string]bool
}

func (m *Master) CreateIntermediaryFile(args *CreateIntermediaryFileArgs, reply *CreateIntermediaryFileReply) error {
	m.reduceWorkQueue.mu.Lock()
		m.reduceWorkQueue.queue[args.Filename] = false
	m.reduceWorkQueue.mu.Unlock()

	return nil
}

func (m *Master) UpdateMapTaskToFinish(args *UpdateMapTaskToFinishArgs, reply *UpdateMapTaskToFinishReply) error {
	m.mapWorkQueue.mu.Lock()

	delete(m.mapWorkQueue.queue, args.Filename)
	if len(m.mapWorkQueue.queue) == 0 {
		fmt.Println("all maps tasks done!", m.mapWorkQueue.queue)
	}

	m.mapWorkQueue.mu.Unlock()

	return nil
}

func (m *Master) UpdateReduceTaskToFinish(args *UpdateReduceTaskToFinishArgs, reply *UpdateReduceTaskToFinishReply) error {
	m.reduceWorkQueue.mu.Lock()
	
	delete(m.reduceWorkQueue.queue, args.Filename)
	if len(m.reduceWorkQueue.queue) == 0 {
		fmt.Println("all reduce tasks done!", m.reduceWorkQueue.queue)
	}
	m.reduceWorkQueue.mu.Unlock()

	return nil
}


func (m *Master) GetNumberOfReduceTasks(args *GetNumberOfReduceTasksArgs, reply *GetNumberOfReduceTasksReply) error {
	reply.NumberOfReduceTasks = m.numberOfReduceTasks
	return nil
}

func (m *Master) GetAvailableReduceInput(args *GetAvailableReduceInputArgs, reply *GetAvailableReduceInputReply) error {
	// if map work queue is not empty, reduce tasks cannot start
	m.mapWorkQueue.mu.Lock()
		// the most hacky shit ever
		// a workaround this weird golang concurrency bug that i have no time to investigate
		// when the last map task is done and removes the file from the mapWorkQueue.queue,
		// the map becomes some weird map with one element (map[:true] or map[:false])
		// =====================================================================
		keys := make([]string, 0, len(m.mapWorkQueue.queue))
		values := make([]bool, 0, len(m.mapWorkQueue.queue))
		for k, v := range m.mapWorkQueue.queue {
			keys = append(keys, k)
			values = append(values, v)
		}
		
		mapTasksDone := len(keys) == 1 && len(values) == 1 && (values[0] == true || values[0] == false)

		if !mapTasksDone {
			fmt.Println("cannot start reduce tasks")
			fmt.Println(len(keys), keys, len(values), values)
			fmt.Println("=============================")

			m.mapWorkQueue.mu.Unlock()
			return nil
		}
		// =====================================================================
	m.mapWorkQueue.mu.Unlock()

	m.reduceWorkQueue.mu.Lock()
		fmt.Println("reduceWorkQueue isnt empty!", m.reduceWorkQueue.queue)
		availableFileName := getNextAvailableFile(m.reduceWorkQueue)
		m.reduceWorkQueue.queue[availableFileName] = true
	m.reduceWorkQueue.mu.Unlock()

	go monitorFile(m.reduceWorkQueue, availableFileName)

	reply.Filename = availableFileName
	return nil
} 

func (m *Master) GetAvailableMapInput(args *GetAvailableMapInputArgs, reply *GetAvailableMapInputReply) error {
	m.mapWorkQueue.mu.Lock()
		availableFileName := getNextAvailableFile(m.mapWorkQueue)
		m.mapWorkQueue.queue[availableFileName] = true
	m.mapWorkQueue.mu.Unlock()

	go monitorFile(m.mapWorkQueue, availableFileName)

	reply.Filename = availableFileName
	return nil
}

func getNextAvailableFile(workQueue WorkQueue) string {
	// find next key in the workqueue such that workQueue[key] is false (not busy)
	
	// assumes calling function has ownership of the Mutex<WorkQueue> and does not lock anything
	availableFilename := ""

	for filename, busy := range workQueue.queue {
		if busy == false {
			availableFilename = filename
			break
		}
	}

	return availableFilename
}

func monitorFile(workQueue WorkQueue, filename string) {
	tenSeconds := time.Millisecond * 1000 * 10
	time.Sleep(tenSeconds)

	workQueue.mu.Lock()
		busy, ok := workQueue.queue[filename]
		if ok {
			if busy {
				// mf crashed yo
				workQueue.queue[filename] = false // set busy status to false so another worker can pick up this work
			} else {
				// its all good, busy is false so another worker can take this file
			}
		} else {
			// its all good, the filename is off the workqueue which means the work has been done
		}
	workQueue.mu.Unlock()
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname) // TODO: why do we remove this socket from the os?
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// fileMap.lock()
	// defer fileMap.unlock()

	// return workQueue.length() === 0



	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		numberOfReduceTasks: nReduce,
		mapWorkQueue: WorkQueue{
			mu: sync.Mutex{},
			queue: make(map[string]bool),
		},
		reduceWorkQueue: WorkQueue{
			mu: sync.Mutex{},
			queue: make(map[string]bool),
		},
	}

	// load mapWorkQueue with filenames
	m.mapWorkQueue.mu.Lock()
	for i := 0; i < len(files); i++ {
		filename := files[i]
		m.mapWorkQueue.queue[filename] = false
	}
	m.mapWorkQueue.mu.Unlock()

	m.server()
	return &m
}
