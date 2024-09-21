/*
 * @Author       : wenwneyuyu
 * @Date         : 2024-09-20 16:41:46
 * @LastEditors  : wenwenyuyu
 * @LastEditTime : 2024-09-21 21:41:37
 * @FilePath     : /src/mr/worker.go
 * @Description  :
 * Copyright 2024 OBKoro1, All Rights Reserved.
 * 2024-09-20 16:41:46
 */
package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		rsp := CallTask()
		switch rsp.RspMsg {
		case MapTask:
			err := MapHandle(mapf, rsp)
			if err == nil {
				_ = Update(MapTask, TaskSuccess, rsp.TaskId)
			} else {
				_ = Update(MapTask, TaskFailed, rsp.TaskId)
			}
		case ReduceTask:
			err := ReduceHandle(reducef, rsp)
			if err == nil {
				_ = Update(ReduceTask, TaskSuccess, rsp.TaskId)
			} else {
				_ = Update(ReduceTask, TaskFailed, rsp.TaskId)
			}
		case WaitTask:
			time.Sleep(time.Second * 10)
		case FinishTask:
			os.Exit(0)
		}

		time.Sleep((time.Second))
	}

}

// 每个HandleMap需要创建nReduce个中间文件
// 中间文件命名mr-X-Y X：map id Y：reduce id
func CallTask() *ResponseMsg {
	args := RequestMsg{
		ReqMsg: CallForTask,
	}

	reply := ResponseMsg{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("in CallTask, reply.nreduce= %v reply.File = %v reply.TaskId = %v reply.nMap = %v \n", reply.NReduce, reply.File, reply.TaskId, reply.NMap)
		return &reply
	} else {
		return nil
	}
}

func Update(fun MsgType, status MsgType, id int) bool {
	args := RequestMsg{
		ReqMsg:     fun,
		TaskStatus: status,
		TaskId:     id,
	}

	reply := ResponseMsg{}
	ok := call("Coordinator.Update", &args, &reply)
	return ok
}

func MapHandle(mapf func(string, string) []KeyValue, rsp *ResponseMsg) error {
	filename := rsp.File
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}

	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	interFiles := make([]*os.File, rsp.NReduce)
	jsonEncoders := make([]*json.Encoder, rsp.NReduce)

	for i := 0; i < rsp.NReduce; i++ {
		interFileName := fmt.Sprintf("mr-tmp-%v-%v", rsp.TaskId, i)
		file, err := os.Create(interFileName)
		if err != nil {
			log.Fatal("cannot create %v: %v", interFileName, err)
			return err
		}
		defer file.Close()

		interFiles[i] = file
		jsonEncoders[i] = json.NewEncoder(file)
	}

	for _, kv := range kva {
		index := ihash(kv.Key) % rsp.NReduce
		err := jsonEncoders[index].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode JSON: %v", err)
			return err
		}
	}

	for _, file := range interFiles {
		file.Close()
	}

	return nil
}

func ReduceHandle(reducef func(string, []string) string, rsp *ResponseMsg) error {
	intermediate := []KeyValue{}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	for i := 0; i < rsp.NMap; i++ {
		interFileName := fmt.Sprintf("mr-tmp-%v-%v", i, rsp.TaskId)
		file, err := os.Open(interFileName)
		if err != nil {
			log.Fatalf("cannot open file %v: %v", file, err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", rsp.TaskId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
