package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

func workerSock() string {
	return fmt.Sprintf("/var/tmp/824-mr-%d", os.Getpid())
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

func (kv *KeyValue) String() string {
	return fmt.Sprintf("%v %v", kv.Key, kv.Value)
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

type WorkerMachine struct {
	SockName string
	Client   *rpc.Client
	ID       int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
	chClose  chan struct{}
}

func (worker *WorkerMachine) Map(args *M2wMapArgs, reply *M2wMapReply) error {
	fmt.Printf("Receive map task: %s\n", args.FilePath)
	go func() {
		var intermediate []KeyValue
		file, err := os.Open(args.FilePath)
		if err != nil {
			log.Fatalf("cannot open %v", args.FilePath)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", args.FilePath)
		}
		file.Close()
		kva := worker.mapf(args.FilePath, string(content))
		intermediate = append(intermediate, kva...)
		sort.Slice(intermediate, func(i, j int) bool {
			return intermediate[i].Key < intermediate[j].Key
		})
		dirName := "map-" + strconv.Itoa(args.TaskID)
		err = os.RemoveAll(dirName)
		if err != nil {
			fmt.Println(err)
			log.Fatal("Can't delete dir: " + dirName)
		}
		os.Mkdir(dirName, 0777)
		files := make([]*bufio.Writer, 0)
		filenames := make([]string, 0)
		for i := 0; i < args.NReduce; i++ {
			filename := path.Join(dirName, dirName+"-"+strconv.Itoa(i))
			file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
			if err != nil {
				fmt.Printf("create err: %v\n", err)
				continue
			}
			filenames = append(filenames, filename)
			defer file.Close()
			w := bufio.NewWriter(file)
			files = append(files, w)
		}
		for i := 0; i < len(intermediate); i++ {
			fileIdx := ihash(intermediate[i].Key) % args.NReduce
			_, err := files[fileIdx].WriteString(fmt.Sprintf("%v %v\n", intermediate[i].Key, intermediate[i].Value))
			if err != nil {
				fmt.Printf("write err: %v\n", err)
				continue
			}
		}
		for i := 0; i < len(files); i++ {
			files[i].Flush()
		}
		fmt.Printf("Finished Map task: %v, output to: %v\n", args.FilePath, dirName)
		worker.CallMapDone(filenames, args.TaskID)
	}()
	return nil
}

func (worker *WorkerMachine) Reduce(args *M2wReduceArgs, reply *M2wReduceReply) error {
	fmt.Printf("Receive reduce task: %v\n", args.TaskID)
	go func() {
		var intermediateKVs []*KeyValue

		for _, filepath := range args.FilePaths {
			file, err := os.OpenFile(filepath, os.O_RDONLY, 0777)
			if err != nil {
				fmt.Printf("failed to read file: %v, %v\n", filepath, err)
				continue
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			scanner.Split(bufio.ScanLines)

			for scanner.Scan() {
				kv := parse(scanner.Text())
				intermediateKVs = append(intermediateKVs, kv)
			}
		}

		sort.Slice(intermediateKVs, func(i, j int) bool {
			return intermediateKVs[i].Key < intermediateKVs[j].Key
		})

		var key string
		var vals []string
		var results []*KeyValue
		for _, kv := range intermediateKVs {
			if key == "" {
				key = kv.Key
				vals = append(vals, kv.Value)
			} else if key == kv.Key {
				vals = append(vals, kv.Value)
			} else {
				v := worker.reducef(key, vals)
				results = append(results, &KeyValue{Key: key, Value: v})
				key = kv.Key
				vals = make([]string, 0)
				vals = append(vals, kv.Value)
			}
		}
		// easy to make mistake
		if len(vals) != 0 {
			v := worker.reducef(key, vals)
			results = append(results, &KeyValue{Key: key, Value: v})
		}

		outputFileName := fmt.Sprintf("mr-out-%v", args.TaskID)
		outputFile, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)

		if err != nil {
			fmt.Printf("failed to create file: %v\n", outputFileName)
			return
		}
		writer := bufio.NewWriter(outputFile)
		for _, v := range results {
			_, err := writer.WriteString(fmt.Sprintf("%v %v\n", v.Key, v.Value))
			if err != nil {
				fmt.Printf("write err: %v\n", err)
				continue
			}
		}
		writer.Flush()

		// notice master
		worker.CallReduceDone(args.TaskID)
		fmt.Printf("Finished Reduce task: %v, output to: %v\n", args.TaskID, outputFileName)
	}()
	return nil
}

func (worker *WorkerMachine) Shutdown(args *M2wShutdownArgs, reply *M2wShutdownReply) error {
	reply.WorkerID = worker.ID
	defer func() {
		time.Sleep(1 * time.Second)
		worker.chClose <- struct{}{}
	}()
	return nil
}

func (worker *WorkerMachine) Ping(args *struct{}, reply *struct{}) error {
	return nil
}

func parse(str string) *KeyValue {
	kv := strings.Split(str, " ")
	return &KeyValue{
		Key:   kv[0],
		Value: kv[1],
	}
}

func (worker *WorkerMachine) CallRegister() {
	args := &W2mRegisterArg{SockName: worker.SockName}
	reply := &W2mRegisterReply{
		Success:  false,
		WorkerID: 0,
	}
	result := worker.call("Master.Register", args, reply)
	if result {
		worker.ID = reply.WorkerID
		fmt.Println("Register success!")
	} else {
		log.Fatal("Register failed!")
	}
}

func (worker *WorkerMachine) CallMapDone(filenames []string, taskID int) {
	args := &W2mMapDoneArg{
		FilePaths: filenames,
		TaskID:    taskID,
		WorkerID:  worker.ID,
	}
	reply := &W2mMapDoneReply{}
	worker.call("Master.MapDone", args, reply)
}

func (worker *WorkerMachine) CallReduceDone(taskID int) {
	args := &W2mReduceDoneArg{TaskID: taskID, WorkerID: worker.ID}
	reply := &W2mReduceDoneReply{}
	worker.call("Master.ReduceDone", args, reply)
}

func (worker *WorkerMachine) server() {
	rpc.Register(worker)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove(worker.SockName)
	l, e := net.Listen("unix", worker.SockName)
	if e != nil {
		log.Fatalf("worker listen %v error: %v", worker.SockName, e)
	}
	fmt.Printf("worker server started on:%s\n", worker.SockName)
	go http.Serve(l, nil)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	worker := WorkerMachine{
		SockName: workerSock(),
		mapf:     mapf,
		reducef:  reducef,
		chClose:  make(chan struct{}),
	}
	client, err := rpc.DialHTTP("unix", masterSock())
	if err != nil {
		log.Fatal("Can't access masker")
		return
	}
	worker.Client = client
	worker.server()
	worker.CallRegister()
	select {
	case <-worker.chClose:
		fmt.Printf("Worker [%v] exits...\n", worker.ID)
		return
	}
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func (worker *WorkerMachine) call(rpcname string, args interface{}, reply interface{}) bool {
	err := worker.Client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("call error: %v\n", err)
	return false
}
