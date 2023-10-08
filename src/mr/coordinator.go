package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.
// TODO 为 worker 编写一个 RPC handlers -> 回复一个尚未开始的映射任务的文件名


// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 启动一个监听 worker.go RPC 调用的线程
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

// main/mrcordinator.go定期调用Done()来查找整个job是否已经完成
func (c *Coordinator) Done() bool {
	ret := false

	// TODO Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// TODO 添加代码来完成协调器的初始化和配置
	// 初始化协调器对象c，可能包括设置一些初始状态和数据结构。
	// 启动一个服务器，这个服务器将用于与工作节点通信。服务器的实现可能会包括设置监听地址和端口，以及定义如何处理来自工作节点的请求和如何响应它们。
	// 返回协调器对象的指针，以便调用方可以与协调器进行交互。

	c.server()
	return &c
}
