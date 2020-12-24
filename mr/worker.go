package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
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
	Id string

	Status      int
	StatusMutex sync.RWMutex

	mapF    func(string, string) []KeyValue
	reduceF func(string, []string) string

	nReduce int

	exit chan struct{}
}

func (d *WorkerDaemon) SetStatus(status int) {
	d.StatusMutex.Lock()
	defer d.StatusMutex.Unlock()
	d.Status = status
}

func (d *WorkerDaemon) AssignTask(args *TaskArgs, reply *EmptyArgsOrReply) error {
	switch args.Type {
	case MapTask:
		{
			log.Printf("Map Task %d is assigned to me", args.Id)
			go d.doMapTask(args.Id, args.Key)

			d.SetStatus(Working)
		}
	case ReduceTask:
		{
			log.Printf("Reduce Task %d is assigned to me", args.Id)
			go d.doReduceTask(args.Id, args.Key, args.MapTaskNum)

			d.SetStatus(Working)
		}
	}
	return nil
}

func (d *WorkerDaemon) doMapTask(id int, key string) {
	file, err := os.Open(key)
	if err != nil {
		log.Printf("fail to open %v\n", key)
		d.SetStatus(Idle)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("fail to read %v\n", key)
		d.SetStatus(Idle)
		return
	}
	file.Close()

	kva := d.mapF(key, string(content))

	reduces := make([][]KeyValue, d.nReduce)

	for _, kv := range kva {
		index := ihash(kv.Key) % d.nReduce
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
			// tmp solution
			d.SetStatus(Idle)
			return
		}
	}
	//time.Sleep(time.Second)
	args, reply := TaskArgs{Id: id, Type: MapTask, Worker: d.Id}, EmptyArgsOrReply{}
	if ok := callMaster("Master.TaskComplete", &args, &reply); ok {
		d.SetStatus(Idle)
	}
}

func (d *WorkerDaemon) doReduceTask(id int, key string, mapNum int) {
	mp := make(map[string][]string)

	for i := 0; i < mapNum; i++ {
		fileName := fmt.Sprintf("mr-%d-%d.json", i, id)
		file, err := os.Open(fileName)
		if err != nil {
			log.Printf("fail to open %v\n", fileName)
			d.SetStatus(Idle)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			mp[kv.Key] = append(mp[kv.Key], kv.Value)
		}
	}

	ofile := fmt.Sprintf("mr-out-%d.txt",id)
	output,err := os.Create(ofile)
	if err!=nil{
		log.Printf("fail to create %v\n",ofile)
		d.SetStatus(Idle)
		return
	}

	for k,v := range mp{
		output.WriteString(fmt.Sprintf("%v %v\n",k,d.reduceF(k,v)))
	}

	output.Close()

	args, reply := TaskArgs{Id: id, Type: ReduceTask, Worker: d.Id}, EmptyArgsOrReply{}
	if ok := callMaster("Master.TaskComplete", &args, &reply); ok {
		d.SetStatus(Idle)
	}

}

func (d *WorkerDaemon) ReportStatusToMaster(status int) {
	args, reply := InterruptArgs{Source: d.Id, Event: "ReportStatus"}, EmptyArgsOrReply{}
	args.Args = strconv.Itoa(status)
	if ok := callMaster("Master.Interrupt", &args, &reply); ok {
	} else {
		log.Println("Report status to master error")
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
		Status:  Initial,
	}

	logfile,_ := os.Create(fmt.Sprintf("mr-worker-%v-log",d.Id))
	log.SetOutput(logfile)

	//Register worker to master
	args, reply := RegisterArgs{Id: d.Id}, RegisterReply{}
	if ok := callMaster("Master.Register", &args, &reply); !ok {
		log.Fatalf("Worker %v failed to register to Master\n", d.Id)
	}
	log.Printf("Worker is registered to master\n")


	d.nReduce = reply.NReduce

	d.server()

	d.SetStatus(Idle)

	go func() {
		for {
			d.StatusMutex.RLock()
			go d.ReportStatusToMaster(d.Status)
			d.StatusMutex.RUnlock()
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
