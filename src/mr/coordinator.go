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

type TaskStatus int
type TaskType int
type MassageType int

const (
	// Task status
	NotStart TaskStatus = iota
	InProgress
	Completed
)

const (
	// Task type
	NotAssigned TaskType = iota
	MapType
	ReduceType
)

type MapInfo struct {
	// in this lab, we don't need to transport file content to worker.
	// worker read exact file by using its file name.
	// the number of map task is equal to the number of input file
	// the number of reduce task is determined by user
	File_name_ string
	Task_id_   int
	Status_    TaskStatus
	Type_      TaskType
	Timestamp_ time.Time // timestamp for re-assign
}

type ReduceInfo struct {
	Task_id_   int
	Status_    TaskStatus
	Type_      TaskType
	Timestamp_ time.Time // timestamp for re-assign
}

type Coordinator struct {
	// Your definitions here.
	map_queue_    []*MapInfo
	reduce_queue_ []*ReduceInfo
	latch_        sync.Mutex // mutex for concurrency
	nReduce_      int
	nMap_         int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) HandleRequest(args *RequestArgs, reply *RequestReply) error {
	//fmt.Println("Coordinator HandleRequest Call")
	c.latch_.Lock()
	for i, e := range c.map_queue_ {
		//fmt.Printf("[map]: idx: %v\n", i)
		if (e.Status_ == NotStart) || (e.Status_ == InProgress && time.Since(c.map_queue_[i].Timestamp_) > time.Second*10) {
			c.map_queue_[i].Timestamp_ = time.Now()
			c.map_queue_[i].Status_ = InProgress
			reply.Map_task_ = *c.map_queue_[i]
			reply.Task_type_ = MapType
			reply.NMap_ = c.nMap_
			reply.NReduce_ = c.nReduce_
			c.latch_.Unlock()
			return nil
		}
	}

	for i, e := range c.reduce_queue_ {
		//fmt.Printf("[reduce]: idx: %v\n", i)
		if (e.Status_ == NotStart) || (e.Status_ == InProgress && time.Since(c.reduce_queue_[i].Timestamp_) > time.Second*10) {
			c.reduce_queue_[i].Timestamp_ = time.Now()
			c.reduce_queue_[i].Status_ = InProgress
			reply.Reduce_task_ = *c.reduce_queue_[i]
			reply.Task_type_ = ReduceType
			reply.NMap_ = c.nMap_
			reply.NReduce_ = c.nReduce_
			c.latch_.Unlock()
			return nil
		}
	}
	// don't have task to assign
	reply.Task_type_ = NotAssigned
	c.latch_.Unlock()
	return nil
}

func (c *Coordinator) HandleRespond(args *RespondArgs, reply *RespondReply) error {
	//fmt.Println("Coordinator HandleRespond Call")
	c.latch_.Lock()
	if args.Task_types == MapType && args.Map_task_.Status_ == Completed {
		c.map_queue_[args.Map_task_.Task_id_].Status_ = Completed
	}
	if args.Task_types == ReduceType && args.Reduce_task_.Status_ == Completed {
		c.reduce_queue_[args.Reduce_task_.Task_id_].Status_ = Completed
	}
	c.latch_.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	c.latch_.Lock()
	for _, e := range c.map_queue_ {
		if e.Status_ != Completed {
			ret = false
			break
		}
	}
	if !ret {
		c.latch_.Unlock()
		return ret
	}
	for _, e := range c.reduce_queue_ {
		if e.Status_ != Completed {
			ret = false
			break
		}
	}

	c.latch_.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	nMap := len(files)

	c.nReduce_ = nReduce
	c.nMap_ = nMap
	//fmt.Printf("[Coordinator]: [nMap]: %v, [nReduce]: %v\n", c.nMap_, c.nReduce_)
	for i := 0; i < nMap; i++ {
		c.map_queue_ = append(c.map_queue_, &MapInfo{File_name_: files[i], Task_id_: i, Status_: NotStart, Type_: MapType})
	}
	for i := 0; i < nReduce; i++ {
		c.reduce_queue_ = append(c.reduce_queue_, &ReduceInfo{Task_id_: i, Status_: NotStart, Type_: ReduceType})
	}

	c.server()
	return &c
}
