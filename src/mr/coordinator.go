/*
 * @Author       : wenwneyuyu
 * @Date         : 2024-09-20 16:41:46
 * @LastEditors  : wenwenyuyu
 * @LastEditTime : 2024-09-21 21:41:23
 * @FilePath     : /src/mr/coordinator.go
 * @Description  :
 * Copyright 2024 OBKoro1, All Rights Reserved.
 * 2024-09-20 16:41:46
 */
package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type State int

const (
	idle State = iota
	running
	finished
	failed
)

type MapInfo struct {
	TaskId    int
	Status    State
	StartTime int64
}

type ReduceInfo struct {
	TaskId    int
	Status    State
	StartTime int64
}

type Coordinator struct {
	// Your definitions here.
	nMap        int
	mapList     map[string]*MapInfo
	mapFinished bool
	mapMutex    sync.Mutex

	nReduce        int
	reduceList     []*ReduceInfo
	reduceFinished bool
	reduceMutex    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *RequestMsg, reply *ResponseMsg) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (c *Coordinator) GetTask(req *RequestMsg, rsp *ResponseMsg) error {
	if req.ReqMsg != CallForTask {
		err := errors.New("Bad request. Call GetTask instead")
		return err
	}

	if !c.mapFinished {
		c.mapMutex.Lock()
		finishedNum := 0
		for file, info := range c.mapList {
			flag := false
			if info.Status == idle || info.Status == failed {
				flag = true
			} else if info.Status == running {
				cur := time.Now().Unix()
				if cur-info.StartTime > 0 {
					flag = true
				}
			} else {
				finishedNum++
			}

			if flag {
				rsp.RspMsg = MapTask
				rsp.TaskId = info.TaskId
				rsp.File = file
				rsp.NReduce = c.nReduce

				info.StartTime = time.Now().Unix()
				info.Status = running
				c.mapMutex.Unlock()
				return nil
			}

		}

		c.mapMutex.Unlock()
		if finishedNum < len(c.mapList) {
			rsp.RspMsg = WaitTask
			return nil
		} else {
			c.mapFinished = true
		}
	}

	if !c.reduceFinished {
		c.reduceMutex.Lock()
		finished := 0
		for idx, info := range c.reduceList {
			flag := false
			if info.Status == idle || info.Status == failed {
				flag = true
			} else if info.Status == running {
				cur := time.Now().Unix()
				if cur-info.StartTime > 10 {
					flag = true
				}
			} else {
				finished++
			}

			if flag {
				rsp.RspMsg = ReduceTask
				rsp.TaskId = idx
				rsp.NMap = c.nMap

				info.StartTime = time.Now().Unix()
				info.Status = running
				c.reduceMutex.Unlock()
				return nil
			}
		}

		c.reduceMutex.Unlock()
		if finished < len(c.reduceList) {
			rsp.RspMsg = WaitTask
			return nil
		} else {
			c.reduceFinished = true
		}
	}

	rsp.RspMsg = FinishTask
	return nil
}

func (c *Coordinator) Update(req *RequestMsg, rsp *ResponseMsg) error {
	if req.ReqMsg == MapTask && !c.mapFinished {
		c.mapMutex.Lock()
		for _, info := range c.mapList {
			if info.TaskId == req.TaskId {
				info.Status = State(req.TaskStatus)
				c.mapMutex.Unlock()
				return nil
			}
		}
		c.mapMutex.Unlock()
	}

	if req.ReqMsg == ReduceTask && !c.reduceFinished {
		c.reduceMutex.Lock()
		for _, info := range c.reduceList {
			if info.TaskId == req.TaskId {
				info.Status = State(req.TaskStatus)
				c.reduceMutex.Unlock()
				return nil
			}
		}
		c.reduceMutex.Unlock()
	}

	return nil
}

func (c *Coordinator) init(files []string) {
	for idx, name := range files {
		c.mapList[name] = &MapInfo{
			TaskId: idx,
			Status: idle,
		}
	}

	for idx := range c.reduceList {
		c.reduceList[idx] = &ReduceInfo{
			TaskId: idx,
			Status: idle,
		}
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.mapFinished && c.reduceFinished {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:       len(files),
		nReduce:    nReduce,
		mapList:    make(map[string]*MapInfo),
		reduceList: make([]*ReduceInfo, nReduce),
	}

	fmt.Printf("nredeuce = %v, c.reduce = %v\n", nReduce, c.nReduce)
	// Your code here.
	c.init(files)
	c.server()
	return &c
}
