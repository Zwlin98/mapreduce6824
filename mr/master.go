package mr

import (
	"fmt"
	"log"
	"strconv"
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

	Phase      int
	PhaseMutex sync.Mutex
	PhaseCond  *sync.Cond

	MapTasks   map[int]Task
	TasksMutex sync.Mutex

	NReduce     int
	ReduceTasks map[int]Task
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

	if args.Type == MapTask {
		m.MapTasks[args.Id].Complete <- struct{}{}
		log.Printf("Map Task %v is Complete\n", args.Id)

	}

	if args.Type == ReduceTask {
		m.ReduceTasks[args.Id].Complete <- struct{}{}
		log.Printf("Reduce Task %v is Complete\n", args.Id)

	}

	return nil
}

func (m *Master) Interrupt(args *InterruptArgs, reply *EmptyArgsOrReply) error {
	switch args.Event {
	case "ReportStatus":

		m.WorkersMutex.Lock()
		status, _ := strconv.Atoi(args.Args)
		m.Workers[args.Source] = status
		m.WorkersMutex.Unlock()

		m.WorkersCond.Broadcast()

		log.Printf("Worker %v is %v", args.Source, WorkerStatus(status))
	}

	return nil
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
	// Your code here.

	m.PhaseMutex.Lock()
	defer m.PhaseMutex.Unlock()

	return m.Phase == Finish

}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	logfile,_ := os.Create("mr-master-log")
	log.SetOutput(logfile)

	m.Workers = make(map[string]int)
	m.WorkersCond = sync.NewCond(&m.WorkersMutex)

	m.Phase = MapPhase
	m.PhaseCond = sync.NewCond(&m.PhaseMutex)

	m.MapTasks = make(map[int]Task)
	for i, file := range files {
		m.MapTasks[i] = Task{Id: i, Key: file, Status: TaskIdle, Complete: make(chan struct{})}
	}

	m.NReduce = nReduce
	m.ReduceTasks = make(map[int]Task)
	for i := 0; i < nReduce; i++ {
		m.ReduceTasks[i] = Task{Id: i, Key: "", Status: TaskIdle, Complete: make(chan struct{})}
	}

	m.server()

	log.Printf("Start to schedule tasks\n")

	go m.scheduleMap()

	go m.scheduleReduce()


	return &m
}

func (m *Master) scheduleMap() {

	getWorker := func(w *string) bool {
		for worker, status := range m.Workers {
			if status == Idle {
				*w = worker
				return true
			}
		}
		return false
	}

	var wg sync.WaitGroup

	for i, t := range m.MapTasks {
		wg.Add(1)

		go func(id int, task Task) {
			//log.Printf("Watching task %v\n", id)
			for task.Status != TaskComplete {
				m.WorkersMutex.Lock()
				//fmt.Printf("Getting Worker!----\n")
				var worker string
				for getWorker(&worker) == false {
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
					wg.Done()
					log.Printf("Map Task %v is Complete\n", id)
				}

			}
		}(i, t)
	}

	wg.Wait()

	m.PhaseMutex.Lock()
	m.Phase = ReducePhase
	m.PhaseMutex.Unlock()

	m.PhaseCond.Signal()

}

func (m *Master) scheduleReduce() {

	m.PhaseMutex.Lock()
	for m.Phase == MapPhase {
		m.PhaseCond.Wait()
	}

	getWorker := func(w *string) bool {
		for worker, status := range m.Workers {
			if status == Idle {
				*w = worker
				return true
			}
		}
		return false
	}

	var wg sync.WaitGroup

	for i, t := range m.ReduceTasks {
		wg.Add(1)

		go func(id int, task Task) {

			for task.Status != TaskComplete {
				m.WorkersMutex.Lock()
				var worker string
				for getWorker(&worker) == false {
					m.WorkersCond.Wait()
				}

				args, reply := TaskArgs{Id: id, Key: strconv.Itoa(id), Type: ReduceTask, MapTaskNum: len(m.MapTasks)}, EmptyArgsOrReply{}
				if ok := callWorker(worker, "WorkerDaemon.AssignTask", &args, &reply); ok {
					log.Printf("Redece Task %v is assigned to %v\n", id, worker)
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
					wg.Done()
					log.Printf("Map Task %v is Complete\n", id)
				}
			}

		}(i, t)
	}

	wg.Wait()


	m.Phase = Finish

	m.PhaseMutex.Unlock()

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
