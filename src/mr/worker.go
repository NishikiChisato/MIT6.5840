package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"sync/atomic"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var (
	ErrInit   = errors.New("Init")
	ErrMap    = errors.New("map fatal")
	ErrReduce = errors.New("reduce fatal")
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	var cur_task_type TaskType = NotAssigned
	var success_call atomic.Value
	success_call.Store(false)
	for {
		req := CallRequest()
		cur_task_type = req.Task_type_
		switch cur_task_type {
		case NotAssigned:
			time.Sleep(time.Second)
		case MapType:
			go func() {
				for success_call.CompareAndSwap(false, false) {
					nor_rsp := ResponseArgs{}
					nor_rsp.Task_types = cur_task_type
					nor_rsp.Map_task_ = MapInfo{File_name_: req.Map_task_.File_name_,
						Task_id_: req.Map_task_.Task_id_,
						Status_:  InProgress,
						Type_:    req.Map_task_.Type_}
					CallResponse(&nor_rsp)
					time.Sleep(5 * time.Second)
				}
			}()
			success_call.Store(MapFunction(&req, mapf))
		case ReduceType:
			go func() {
				for success_call.CompareAndSwap(false, false) {
					nor_rsp := ResponseArgs{}
					nor_rsp.Task_types = cur_task_type
					nor_rsp.Reduce_task_ = ReduceInfo{Task_id_: req.Reduce_task_.Task_id_,
						Status_: InProgress,
						Type_:   req.Reduce_task_.Type_}
					CallResponse(&nor_rsp)
					time.Sleep(5 * time.Second)
				}
			}()
			success_call.Store(ReduceFunction(&req, reducef))
		}

		if cur_task_type != NotAssigned && success_call.CompareAndSwap(true, false) {
			rsp_args := ResponseArgs{}
			rsp_args.Task_types = cur_task_type
			switch cur_task_type {
			case MapType:
				rsp_args.Map_task_ = MapInfo{File_name_: req.Map_task_.File_name_,
					Task_id_: req.Map_task_.Task_id_,
					Status_:  Completed,
					Type_:    req.Map_task_.Type_}
				CallResponse(&rsp_args)
			case ReduceType:
				rsp_args.Reduce_task_ = ReduceInfo{Task_id_: req.Reduce_task_.Task_id_,
					Status_: Completed,
					Type_:   req.Reduce_task_.Type_}
				CallResponse(&rsp_args)
			}
		}
	}

}

func MapFunction(req *RequestReply, mapf func(string, string) []KeyValue) bool {
	file, err := os.Open(req.Map_task_.File_name_)
	if err != nil {
		//log.Fatalf("cannot open %v\n", req.Map_task_.File_name_)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		//log.Fatalf("cannot read %v\n", req.Map_task_.File_name_)
		return false
	}
	file.Close()
	kva := mapf(req.Map_task_.File_name_, string(content))
	groups := map[int][]KeyValue{}
	for _, kv := range kva {
		groups[ihash(kv.Key)%req.NReduce_] = append(groups[ihash(kv.Key)%req.NReduce_], kv)
	}
	// create tmp file list and encoder list
	tfile_list := []*os.File{}
	enc_list := []*json.Encoder{}
	for i := 0; i < req.NReduce_; i++ {
		//tfile, _ := os.Create(tfilename)
		tfile, _ := os.CreateTemp("./", "tmp-mr-")
		defer tfile.Close()
		tfile_list = append(tfile_list, tfile)
		enc_list = append(enc_list, json.NewEncoder(tfile))
	}

	// encode
	for idx, vals := range groups {
		enc := enc_list[idx]
		for _, kv := range vals {
			err := enc.Encode(&kv)
			if err != nil {
				//log.Fatalf("encode error: [file name]: %v\n", tfile_list[idx])
				return false
			}
		}
	}
	for i := 0; i < req.NReduce_; i++ {
		tfilename := fmt.Sprintf("mr-%d-%d", req.Map_task_.Task_id_, i)
		if err := os.Rename(tfile_list[i].Name(), tfilename); err != nil {
			return false
		}
	}
	return true
}

func ReduceFunction(req *RequestReply, reducef func(string, []string) string) bool {
	kva := []KeyValue{}
	for i := 0; i < req.NMap_; i++ {
		ifilename := fmt.Sprintf("mr-%d-%d", i, req.Reduce_task_.Task_id_)
		ifile, err := os.Open(ifilename)
		if err != nil {
			//log.Fatalf("cannot open %v\n", ifilename)
			return false
		}
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	ofilename := fmt.Sprintf("mr-out-%d", req.Reduce_task_.Task_id_)
	ofile, _ := os.CreateTemp("./", "tmp-mr-out-")
	defer ofile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	if err := os.Rename(ofile.Name(), ofilename); err != nil {
		return false
	}

	return true
}

func CallRequest() RequestReply {
	args := RequestArgs{}
	reply := RequestReply{}
	ok := call("Coordinator.HandleRequest", &args, &reply)

	/*
		if ok {
			log.Println("request success")
			if reply.Task_type_ == MapType {
				log.Printf("[Map] [file name]: %v, [task id]: %v, [status]: %v\n", reply.Map_task_.File_name_, reply.Map_task_.Task_id_, reply.Map_task_.Status_)
			}
			if reply.Task_type_ == ReduceType {
				log.Printf("[Reduce] [task id]: %v", reply.Map_task_.Task_id_)
			}
		} else {
			log.Fatalln("request failed")
		}
	*/
	if !ok {
		//log.Fatalln("request failed")
		os.Exit(-1)
	}
	return reply
}

func CallResponse(args *ResponseArgs) {
	reply := ResponseReply{}
	ok := call("Coordinator.HandleResponse", args, &reply)
	/*
		if ok {
			log.Println("respond success")
		} else {
			log.Fatalln("respond failed")
		}
	*/
	if !ok {
		//log.Fatalln("respond failed")
		os.Exit(-1)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
		// remote is close
		os.Exit(-1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
