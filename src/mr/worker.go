package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 定义 SortedKey ,
type SortedKey []KeyValue

func (s SortedKey) Len() int           { return len(s) }
func (s SortedKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SortedKey) Less(i, j int) bool { return s[i].Key < s[j].Key }

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
				DoReduceTask(reducef, &task)
				callDone(&task)
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
	filename := response.FileSlice[0]
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

func DoReduceTask(reducef func(string, []string) string, response *Task) {
	input := shuffle(response.FileSlice)
	// 获取当前工作目录
	dir, _ := os.Getwd()
	// 创建存储 reduce 输出结果的临时文件
	outputTempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create reduce output file", err)
	}
	i := 0
	for i < len(input) {
		// 具有相同 Key 的数据项的结束位置
		sameKeyIndex := i
		for sameKeyIndex < len(input) && input[sameKeyIndex].Key == input[i].Key {
			sameKeyIndex++
		}
		var values []string
		for k := i; k < sameKeyIndex; k++ {
			// 遍历具有相同 Key 的数据项，将它们的值添加到 values 中
			values = append(values, input[k].Value)
		}
		// 调用用户提供的 reducef 函数，传递当前 Key 和对应的值列表 values，以执行 Reduce 操作，将结果存储在 output 变量中
		outputValue := reducef(input[i].Key, values)
		// 将 Reduce 操作的结果格式化为 "Key Value\n" 的形式并写入临时文件，
		fmt.Fprintf(outputTempFile, "%v %v\n", input[i].Key, outputValue)
		i = sameKeyIndex
	}
	outputTempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", response.TaskId)
	// 将临时文件重命名为最终的输出文件名，完成 Reduce 阶段的任务
	os.Rename(outputTempFile.Name(), fn)
}

// 传入文件名数组,返回排序好的 kv 数组
func shuffle(files []string) []KeyValue {
	var resKv []KeyValue
	for _, filename := range files {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// 使用JSON解码器 dec 从文件中解码一个JSON对象，并将其存储到 kv 变量中
			if err := dec.Decode(&kv); err != nil {
				break
			}
			resKv = append(resKv, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKey(resKv))
	return resKv
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
		fmt.Println("callDone(): close task", finishedTask)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}
