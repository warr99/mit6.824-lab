package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "io/ioutil"
import "strings"
import "strconv"

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	TaskId            int           //自增的id,每一个任务都是通过Coordinator初始化的,这个属性主要是为了给Task分配唯一id
	ReducerNum        int           // reducer 数量
	files             []string      // 传入的文件数组
	TaskChannelReduce chan *Task    // Reduce 任务
	TaskChannelMap    chan *Task    // Map 任务
	TaskMap           map[int]*Task // 任务 map
	Phase             Phase         // 程序所处阶段(map/reduce/done)
}

// Your code here -- RPC handlers for the worker to call.
// TODO 为 worker 编写一个 RPC handlers -> 分配任务
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	// 判断当前程序处于哪个阶段
	switch c.Phase {
	// 处于 map 阶段
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				if !c.judgeState(reply.TaskId) {
					// task 不处于 Waiting 状态
					fmt.Printf("Map-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				// TaskChannelMap 中没有空闲的 map 节点可以使用了
				reply.TaskType = WaittingTask
				if c.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}

		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				fmt.Printf("poll-Reduce-taskid[ %d ]\n", reply.TaskId)
				if !c.judgeState(reply.TaskId) {
					fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask // 如果reduce任务被分发完了但是又没完成，此时就将任务设为Waitting
				if c.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	default:
		{
			panic("the phase undifined")
		}
	}
	return nil
}

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
	mu.Lock()
	defer mu.Unlock()
	if c.Phase == AllDone {
		fmt.Println("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		TaskMap:           make(map[int]*Task, len(files)+nReduce),
		Phase:             MapPhase,
	}
	c.makeMapTasks(files)
	c.server()
	return &c
}

// 对map任务进行处理,初始化map任务
func (c *Coordinator) makeMapTasks(files []string) {

	for _, v := range files {
		// 生成唯一 id
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v},
			State:      Waiting,
		}
		c.TaskMap[id] = &task
		fmt.Println("make a map task :", &task)
		c.TaskChannelMap <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i),
		}
		c.TaskMap[id] = &task
		fmt.Println("make a reduce task :", &task)
		c.TaskChannelReduce <- &task
	}
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (c *Coordinator) judgeState(taskId int) bool {
	task, ok := c.TaskMap[taskId]
	if !ok || task.State != Waiting {
		return false
	}
	task.State = Working
	return true
}

// 判断是否需要切换到下一阶段 map->reduce ...
func (c *Coordinator) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	for _, v := range c.TaskMap {
		if v.TaskType == MapTask {
			if v.State == Done {
				mapDoneNum++
			} else {
				mapDoneNum++
			}
		} else if v.TaskType == ReduceTask {
			if v.State == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	// map阶段的全部完成,reduce阶段的全部都还没开始
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		// reduce阶段的全部完成
		return true
	}
	return false
}

func (c *Coordinator) toNextPhase() {
	// 当前在 map 阶段 map -> reduce
	if c.Phase == MapPhase {
		c.makeReduceTasks()
		c.Phase = ReducePhase
		// 当前在 reduce 阶段 reduce -> AllDone
	} else if c.Phase == ReducePhase {
		fmt.Println("set the phase -> allDone")
		c.Phase = AllDone
	}
}

// 提供一个RPC调用,标记当前任务已经完成
func (c *Coordinator) SetTaskDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		task, ok := c.TaskMap[args.TaskId]
		if ok && task.State == Working {
			task.State = Done
			fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] is already finished.\n", args.TaskId)
		}
	case ReduceTask:
		task, ok := c.TaskMap[args.TaskId]
		if ok && task.State == Working {
			task.State = Done
			fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Reduce task Id[%d] is already finished.\n", args.TaskId)
		}
	default:
		panic("The task type undefined ! ! !")
	}
	return nil
}

// 从当前工作目录中读取文件列表，并选择map生成的temp文件
func selectReduceName(reduceNum int) []string {
	var s []string
	// 获取当前工作目录
	path, _ := os.Getwd()
	// 获取当前工作目录下的所有文件
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 以 "mr-tmp" 开头并且以 reduceNum 结尾
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}
