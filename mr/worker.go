package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "./uuid"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerDaemon struct {
	Id      string

	Status	int
	StatusMutex sync.Mutex

	mapF    func(string, string) []KeyValue
	reduceF func(string, []string) string

	nReduce int

	exit    chan struct{}
}

func (d *WorkerDaemon) AssignTask(args *TaskArgs, reply *EmptyArgsOrReply) error {
	switch args.Type {
	case MapTask:
		{
			log.Printf("Map Task %d is assigned to me", args.Id)
			go d.doMapTask(args.Id, args.Key)
			d.StatusMutex.Lock()
			d.Status = Working
			d.StatusMutex.Unlock()
		}
	}
	return nil
}

func (d *WorkerDaemon) doMapTask(id int, key string) {
	file, err := os.Open(key)
	if err != nil {
		log.Printf("fail to open %v\n", key)
		//TODO: Report Error to Master
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("fail to read %v\n", key)
		//TODO: Report Error To Master
	}
	file.Close()

	kva := d.mapF(key, string(content))

	reduces := make([][]KeyValue, d.nReduce)

	for _, kv := range kva {
		index := ihash(kv.Key) %d.nReduce
		reduces[index] = append(reduces[index], kv)
	}

	for index, values := range reduces {
		ofile := fmt.Sprintf("mr-%d-%d.json", id, index)
		if outfile, err := os.Create(ofile); err == nil {
			enc := json.NewEncoder(outfile)
			for _, value := range values {
				enc.Encode(&value)
			}
		} else {
			log.Printf("fail to create %v\n", ofile)
			//TODO: Report error to Master
		}
	}
	//time.Sleep(time.Second)
	args, reply := TaskArgs{Id: id, Type: MapTask, Worker: d.Id}, EmptyArgsOrReply{}
	if ok := callMaster("Master.TaskComplete", &args, &reply); ok {
		d.StatusMutex.Lock()
		d.Status = Idle
		d.StatusMutex.Unlock()
		//TODO: free worker to execute another task
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	d := WorkerDaemon{
		Id:      fmt.Sprintf("/var/tmp/local-worker-%v", uuid.New().String()),
		mapF:    mapf,
		reduceF: reducef,
		exit:    make(chan struct{}),
		Status: Initial,
	}

	//Register worker to master
	args, reply := RegisterArgs{Id: d.Id}, RegisterReply{}
	if ok := callMaster("Master.Register", &args, &reply); !ok {
		log.Fatalf("Worker %v failed to register to Master\n", d.Id)
	}
	log.Printf("Worker is registered to master\n")

	fmt.Println(reply.NReduce)
	d.nReduce = reply.NReduce

	d.server()

	d.StatusMutex.Lock()
	d.Status = Idle
	d.StatusMutex.Unlock()

	go func() {
		for {
			d.StatusMutex.Lock()
			if d.Status == Idle{
				args, reply := TaskArgs{Id: 986473, Type: MapTask, Worker: d.Id}, EmptyArgsOrReply{}
				if ok := callMaster("Master.TaskComplete", &args, &reply); ok {
					//TODO: free worker to execute another task
				}
			}
			d.StatusMutex.Unlock()
			time.Sleep(time.Second)
		}
	}()

	// Your worker implementation here.
	for range d.exit {
		return
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (d *WorkerDaemon) server() {
	rpc.Register(d)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := d.Id
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func callMaster(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
