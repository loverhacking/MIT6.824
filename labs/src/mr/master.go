package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

const taskTimeout = time.Second * 10

type MasterPhase int

const (
	MapPhase MasterPhase = iota
	ReducePhase
	DonePhase
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type TaskMeta struct {
	TaskType  TaskType
	TaskID    int
	State     TaskState
	StartTime time.Time
	Filename  string
}

type Master struct {
	mu          sync.Mutex
	phase       MasterPhase
	nReduce     int
	nMap        int
	mapTasks    []TaskMeta
	reduceTasks []TaskMeta
}

type WorkerArgs struct {
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch m.phase {
	case MapPhase:
		if m.assignTask(m.mapTasks, reply) {
			return nil
		}

	case ReducePhase:
		if m.assignTask(m.reduceTasks, reply) {
			return nil
		}
	case DonePhase:
		reply.TaskType = ExitTask
		return nil
	}

	reply.TaskType = WaitTask
	reply.NMap = m.nMap
	reply.NReduce = m.nReduce
	return nil
}

func (m *Master) assignTask(tasks []TaskMeta, reply *AskTaskReply) bool {
	for i := range tasks {
		task := &tasks[i]

		if task.State == Idle || m.shouldReassign(task) {
			task.State = InProgress
			task.StartTime = time.Now()
			m.fillTaskReply(reply, task)
			return true
		}
	}
	return false
}

func (m *Master) shouldReassign(task *TaskMeta) bool {
	// if a task InProgress but has been running for more than 10 seconds,
	// we assume the worker has failed and reassign the task
	return task.State == InProgress && time.Since(task.StartTime) > taskTimeout
}

func (m *Master) fillTaskReply(reply *AskTaskReply, task *TaskMeta) {
	reply.TaskID = task.TaskID
	reply.TaskType = task.TaskType
	reply.FileName = task.Filename
	reply.NMap = m.nMap
	reply.NReduce = m.nReduce
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		// should check the current master phase to avoid timeout worker very late report
		if m.phase != MapPhase {
			return nil
		}
		if completeTask(m.mapTasks, args.TaskID) {
			reply.Ok = true
		}

		if allTasksDone(m.mapTasks) {
			m.phase = ReducePhase
		}

		return nil

	case ReduceTask:
		if m.phase != ReducePhase {
			return nil
		}
		if completeTask(m.reduceTasks, args.TaskID) {
			reply.Ok = true
		}

		if allTasksDone(m.reduceTasks) {
			m.phase = DonePhase
		}
		return nil
	}

	return nil
}

func completeTask(tasks []TaskMeta, taskID int) bool {
	if taskID < 0 || taskID >= len(tasks) {
		return false
	}
	tasks[taskID].State = Completed
	return true
}

func allTasksDone(tasks []TaskMeta) bool {
	for i := range tasks {
		if tasks[i].State != Completed {
			return false
		}
	}
	return true
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.phase == DonePhase
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		phase:       MapPhase,
		nReduce:     nReduce,
		nMap:        len(files),
		mapTasks:    make([]TaskMeta, len(files)),
		reduceTasks: make([]TaskMeta, nReduce),
	}

	for i, filename := range files {
		m.mapTasks[i] = TaskMeta{
			TaskType: MapTask,
			TaskID:   i,
			State:    Idle,
			Filename: filename,
		}
	}

	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = TaskMeta{
			TaskType: ReduceTask,
			TaskID:   i,
			State:    Idle,
		}
	}

	m.server()
	return &m
}
