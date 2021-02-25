package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type CreateIntermediaryFileArgs struct {
	Filename string
}

type CreateIntermediaryFileReply struct {

}

type UpdateMapTaskToFinishArgs struct {
	Filename string
}

type UpdateMapTaskToFinishReply struct {

}

type GetAvailableMapInputArgs struct {

}

type GetAvailableMapInputReply struct {
	Filename string
}

type GetAvailableReduceInputArgs struct {

}

type GetAvailableReduceInputReply struct {
	Filename string
}

type GetNumberOfReduceTasksArgs struct {

}

type GetNumberOfReduceTasksReply struct {
	NumberOfReduceTasks int
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())	
	return s
}
