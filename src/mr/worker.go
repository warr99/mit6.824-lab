package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strconv"
import "time"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	flag := true
	for flag {
		// send an RPC to the coordinator asking for a task
		task := getTask()
		switch task.TaskType {
		case MapTask:
			{
				DoMapTask(mapf, &task)
				callDone(&task)
			}
		case ReduceTask:
			{
				// TODO 执行相应的 ReduceTask
			}
		case WaittingTask:
			{
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
		case ExitTask:
			{
				fmt.Println("exit")
				flag = false
			}
		}

	}

}

func getTask() Task {
	reqArgs := TaskArgs{}
	replyTask := Task{}
	ok := call("Coordinator.PollTask", &reqArgs, &replyTask)
	if ok {
		fmt.Println("replyTask: ", replyTask)
	} else {
		fmt.Printf("call failed!\n")
	}
	return replyTask
}

func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	filename := response.Filename
	var mapRes []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// 通过io工具包获取conten,作为mapf的参数
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	mapRes = mapf(filename, string(content))
	reducerNum := response.ReducerNum
	// 遍历 mapRes 中的键值对，对每个键值对进行哈希操作（通过 ihash 函数），然后根据哈希值将键值对添加到对应的 Reducer 的切片中
	// 创建一个长度为nReduce的二维切片
	HashedKV := make([][]KeyValue, reducerNum)
	for _, kv := range mapRes {
		HashedKV[ihash(kv.Key)%reducerNum] = append(HashedKV[ihash(kv.Key)%reducerNum], kv)
	}
	// 循环遍历每个 Reducer，为每个 Reducer 创建一个临时文件，并将属于该 Reducer 的键值对写入该文件中，使用 JSON 编码
	for i := 0; i < reducerNum; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		fmt.Println("make a temp file ", oname)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

	fmt.Println("err: ", err)
	return false
}

// callDone Call RPC to mark the task as completed
func callDone(finishedTask *Task) Task {

	reply := Task{}
	ok := call("Coordinator.SetTaskDone", finishedTask, &reply)

	if ok {
		fmt.Println("callDone(): close task", &finishedTask)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}
