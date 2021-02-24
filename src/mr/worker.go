package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	var wg sync.WaitGroup

	wg.Add(1)
	go mapTask(mapf, &wg)
	go reduceTask(reducef)

	wg.Wait()
}

func mapTask(mapf func(string, string) []KeyValue, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Println("running map task")
	fileNameForMapTask := GetAvailableFilenameForMapTask()
	NReduce := GetNumberOfReduceTasks()

	for fileNameForMapTask != "" {
		// intermediate := []KeyValue{}

		file, err := os.Open(fileNameForMapTask)
		if err != nil {
			log.Fatalf("cannot open %v", fileNameForMapTask)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileNameForMapTask)
		}
		file.Close()

		kva := mapf(fileNameForMapTask, string(content))

		for i := 0; i < len(kva); i++ {
			reduceTaskNumber := ihash(kva[i].Key) % NReduce
			oname := "mr-" /* + string(mapTaskNumber)*/ + strconv.Itoa(reduceTaskNumber)
			
			// TODO: use temp files
			
			var ofile *os.File
			
    		_, fileError := os.Stat(oname)
    		if os.IsNotExist(fileError) {
				ofile, _ = os.Create(oname)
			} else {
				ofile, err = os.OpenFile(oname, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
				
				if err != nil {
					fmt.Println(err)
				}
			}

			fmt.Println("placing in", oname, kva[i])
			enc := json.NewEncoder(ofile)
			encodeError := enc.Encode(&kva[i])
			ofile.Close()
			
			if encodeError != nil {
				// TODO: throw?
				fmt.Println(encodeError)
			}
		}

		fmt.Println("done")
		break

		// TODO: MARK JOB AS DONE
	}
}

func reduceTask(reducef func(string, []string) string) {
	// availableIntermediateFilesToReduce := RPCCallToMaster()
	
	// while availableIntermediateFilesToReduce {
		// intermediate := thing we need from fs

		// oname := "mr-0-0"
		// ofile, _ := os.Create(oname)
		// fmt.Fprintf(ofile, "%v", intermediate)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		// i := 0
		// for i < len(intermediate) {
		// 	j := i + 1
		// 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
		// 		j++
		// 	}
		// 	values := []string{}
		// 	for k := i; k < j; k++ {
		// 		values = append(values, intermediate[k].Value)
		// 	}
		// 	output := reducef(intermediate[i].Key, values)

		// 	// this is the correct format for each line of Reduce output.
		// 	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		// 	i = j
		// }
	// }
}

func GetAvailableFilenameForMapTask() string {
	args := GetAvailableMapInputArgs {}
	reply := GetAvailableMapInputReply {}

	call("Master.GetAvailableMapInput", &args, &reply)
	fmt.Println(reply.Filename)

	return reply.Filename
}

func GetNumberOfReduceTasks() int {
	args := GetNumberOfReduceTasksArgs {}
	reply := GetNumberOfReduceTasksReply {}
	
	call("Master.GetNumberOfReduceTasks", &args, &reply)

	return reply.NumberOfReduceTasks
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
