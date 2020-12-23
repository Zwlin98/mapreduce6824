package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	Workers      map[string]int
	WorkersMutex sync.Mutex
	WorkersCond  *sync.Cond

	Phase        int

	Tasks      map[int]Task
	TasksMutex sync.Mutex

	NReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {

	m.WorkersMutex.Lock()
	m.Workers[args.Id] = Initial
	m.WorkersMutex.Unlock()


	reply.NReduce = m.NReduce
	log.Printf("Worker %v is Registered\n", args.Id)

	return nil
}

func (m *Master) TaskComplete(args *TaskArgs, reply *EmptyArgsOrReply) error {

	m.Tasks[args.Id].Complete <- struct{}{}

	m.WorkersMutex.Lock()
	m.Workers[args.Worker] = Idle
	m.WorkersMutex.Unlock()

	m.WorkersCond.Broadcast()

	log.Printf("Task %v is Complete\n", args.Id)

	return nil
}

func (m *Master) Interrupt()  {

}
//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.NReduce = nReduce

	m.Workers = make(map[string]int)

	m.Phase = MapPhase

	m.Tasks = make(map[int]Task)
	for i, file := range files {
		m.Tasks[i] = Task{Id: i, Key: file, Status: TaskIdle, Complete: make(chan struct{})}
	}

	m.WorkersCond = sync.NewCond(&m.WorkersMutex)

	m.server()

	log.Printf("Start to schedule tasks\n")
	m.scheduler()

	return &m
}

func (m *Master) scheduler() bool {


	getWorker := func(w *string) bool {
		for worker, status := range m.Workers {
			if status == Idle {
				*w = worker
				return true
			}
		}
		return false
	}

	for i, t := range m.Tasks {

		go func(id int, task Task) {
			//log.Printf("Watching task %v\n", id)
			for task.Status != TaskComplete {
				m.WorkersMutex.Lock()
				//fmt.Printf("Getting Worker!----\n")
				var worker string
				for getWorker(&worker)==false {
					m.WorkersCond.Wait()
				}

				args, reply := TaskArgs{Id: task.Id, Key: task.Key, Type: MapTask}, EmptyArgsOrReply{}
				if ok := callWorker(worker, "WorkerDaemon.AssignTask", &args, &reply); ok {
					log.Printf("Map Task %v is assigned to %v\n", id, worker)
					task.Status = TaskInProgress
					m.Workers[worker] = Working
				} else {
					m.Workers[worker] = Unknown
					//TODO: handle call error
				}

				m.WorkersMutex.Unlock()

				select {
				case <-time.After(10 * time.Second):
					task.Status = Idle
					log.Printf("Map Task %v is Out Of time\n", id)
				case <-task.Complete:
					task.Status = TaskComplete
					log.Printf("Map Task %v is Complete\n", id)
				}

			}
		}(i, t)
	}

	return false
}

func callWorker(worker, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", worker)
	if err != nil {
		log.Printf("dialing: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
