package mr

import (
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
	workQueue WorkQueue // TODO: do we need to capitalize and export?
}

type WorkQueue struct {
	mu sync.Mutex
	queue map[string]bool
}

// Your code here -- RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) UpdateMapTaskToFinish(args *UpdateMapTaskToFinishArgs, reply *UpdateMapTaskToFinishReply) error {
	m.workQueue.mu.Lock()
		delete(m.workQueue.queue, args.Filename)
	m.workQueue.mu.Unlock()

	return nil
}

func (m *Master) GetNumberOfReduceTasks(args *GetNumberOfReduceTasksArgs, reply *GetNumberOfReduceTasksReply) error {
	reply.NumberOfReduceTasks = m.numberOfReduceTasks
	return nil
}

func (m *Master) GetAvailableMapInput(args *GetAvailableMapInputArgs, reply *GetAvailableMapInputReply) error {
	m.workQueue.mu.Lock()
		availableFileName := getNextAvailableFile(m.workQueue)
		m.workQueue.queue[availableFileName] = true
	m.workQueue.mu.Unlock()

	go monitorFile(m.workQueue, availableFileName)

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
		workQueue: WorkQueue{
			mu: sync.Mutex{},
			queue: make(map[string]bool),
		},
	}

	m.workQueue.mu.Lock()
	for i := 0; i < len(files); i++ {
		filename := files[i]
		m.workQueue.queue[filename] = false
	}
	m.workQueue.mu.Unlock()

	m.server()
	return &m
}
