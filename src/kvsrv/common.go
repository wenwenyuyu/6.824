/*
 * @Author       : wenwneyuyu
 * @Date         : 2024-09-20 16:41:46
 * @LastEditors  : wenwenyuyu
 * @LastEditTime : 2024-09-23 18:10:24
 * @FilePath     : /src/kvsrv/common.go
 * @Description  :
 * Copyright 2024 OBKoro1, All Rights Reserved.
 * 2024-09-20 16:41:46
 */
package kvsrv

type Type int

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID     int64
	Counts uint64
}

type PutAppendReply struct {
	Value  string
	Counts uint64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID int64
}

type GetReply struct {
	Value string
}
