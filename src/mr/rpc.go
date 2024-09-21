/*
 * @Author       : wenwneyuyu
 * @Date         : 2024-09-20 16:41:46
 * @LastEditors  : wenwenyuyu
 * @LastEditTime : 2024-09-21 21:41:15
 * @FilePath     : /src/mr/rpc.go
 * @Description  :
 * Copyright 2024 OBKoro1, All Rights Reserved.
 * 2024-09-20 16:41:46
 */
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

type MsgType int

const (
	CallForTask MsgType = iota
	MapTask
	ReduceTask
	WaitTask
	FinishTask
	CallError
	TaskSuccess
	TaskFailed
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequestMsg struct {
	ReqMsg     MsgType
	TaskStatus MsgType
	TaskId     int
}

type ResponseMsg struct {
	RspMsg  MsgType
	TaskId  int
	File    string
	NReduce int
	NMap    int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
