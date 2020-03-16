package mr

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	nReduce int
	// workers
	workers         []*worker
	activeWorkerNum int
	workersLock     sync.RWMutex
	zombieWorkers   chan *worker
	idleWorkers     chan *worker
	//tasks
	mapTasks      []*mapTask
	chMapTasks    chan *mapTask
	reduceTasks   []*reduceTask
	chReduceTasks chan *reduceTask

	finishedMapTasks      int64
	finishedReduceTasks   int64
	intermediateFiles     [][]string
	intermediateFilesLock sync.RWMutex

	ChClose chan struct{}
}

type mapTask struct {
	fileName     string
	chJobSuccess chan struct{}
	done         atomic.Value
	TaskID       int
}

type reduceTask struct {
	filePaths    []string
	chJobSuccess chan struct{}
	done         atomic.Value
	TaskID       int
}

// ****************** RPC methods ******************
func (m *Master) Register(args *W2mRegisterArg, reply *W2mRegisterReply) error {
	w := &worker{}
	client, err := rpc.DialHTTP("unix", args.SockName)
	if err != nil {
		reply.Success = false
		fmt.Println(err)
		return err
	}
	m.workersLock.Lock()
	m.workers = append(m.workers, w)
	reply.WorkerID = len(m.workers) - 1
	w.id = len(m.workers) - 1
	w.sockName = args.SockName
	w.online = true
	fmt.Printf("Registered a new worker, current active worker number: %d\n", m.activeWorkerNum)
	m.activeWorkerNum++
	m.workersLock.Unlock()
	w.client = client

	m.idleWorkers <- w
	reply.Success = true
	return nil
}

func (m *Master) MapDone(args *W2mMapDoneArg, reply *W2mMapDoneReply) error {
	m.intermediateFilesLock.Lock()
	defer m.intermediateFilesLock.Unlock()
	if m.mapTasks[args.TaskID].done.Load() == true {
		fmt.Printf("decayed MapDone, taskID: %v\n", args.TaskID)
		return fmt.Errorf("decayed MapDone, taskID: %v\n", args.TaskID)
	}

	// notify
	m.mapTasks[args.TaskID].done.Store(true)
	m.mapTasks[args.TaskID].chJobSuccess <- struct{}{}

	for i := 0; i < len(args.FilePaths); i++ {
		m.intermediateFiles[i] = append(m.intermediateFiles[i], args.FilePaths[i])
	}

	// put the worker back to work
	m.idleWorkers <- m.workers[args.WorkerID]
	return nil
}

func (m *Master) ReduceDone(args *W2mReduceDoneArg, reply *W2mReduceDoneReply) error {
	if m.reduceTasks[args.TaskID].done.Load() == true {
		fmt.Println("decayed ReduceDone")
		return fmt.Errorf("decayed ReduceDone")
	}
	// notify
	m.reduceTasks[args.TaskID].chJobSuccess <- struct{}{}
	m.reduceTasks[args.TaskID].done.Store(true)

	// put the worker back to work
	m.idleWorkers <- m.workers[args.WorkerID]

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
	fmt.Printf("server started on:%s\n", sockname)
	go http.Serve(l, nil)
}

func (m *Master) revive() {
outer:
	for {
		select {
		case <-m.ChClose:
			break outer
		case <-time.After(time.Second * 1):
			w := <-m.zombieWorkers
			if w.ping() == true {
				m.idleWorkers <- w
			} else {
				w.retry++
				if w.retry < 3 {
					//online again, put it back to worker list
					m.zombieWorkers <- w
				} else {
					fmt.Printf("Worker %v is not responding within 10sec. It will be removed from worker list\n", w.id)
					m.workersLock.Lock()
					// delete offline worker
					//TODO remove offline worker from worker list
					w.online = false
					m.activeWorkerNum--
					m.workersLock.Unlock()
				}
			}
		}
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	select {
	case <-m.ChClose:
		fmt.Println("Master exists...")
		return true
	}
}

func (m *Master) dispatch() {
	// mapper tasks
	for mTask := range m.chMapTasks {
		idleWorker := <-m.idleWorkers
		go idleWorker.callMap(mTask, m)
	}

	fmt.Printf("All the MapTasks are finished, start to dispatch ReduceTasks")

	// reduce tasks
	for i := 0; i < len(m.intermediateFiles); i++ {
		rTask := &reduceTask{
			filePaths:    m.intermediateFiles[i],
			chJobSuccess: make(chan struct{}),
			done:         atomic.Value{},
			TaskID:       i,
		}
		m.reduceTasks = append(m.reduceTasks, rTask)
		m.chReduceTasks <- rTask
	}
	for rTask := range m.chReduceTasks {
		idleWorker := <-m.idleWorkers
		go idleWorker.callReduce(rTask, m)
	}

	fmt.Printf("All the ReduceTasks are finished, start to shutdown all the workers\n")

	for _, w := range m.workers {
		w.shutdown()
	}

	m.ChClose <- struct{}{}
}

type worker struct {
	online   bool
	retry    int
	id       int
	sockName string
	client   *rpc.Client
}

func (w *worker) ping() bool {
	args := &struct {
	}{}
	reply := &struct {
	}{}
	return w.call("WorkerMachine.Ping", args, reply)
}

func (w *worker) callMap(mTask *mapTask, master *Master) {
	fmt.Printf("Dispatch [MapTask %v %v] to worker [%v %v]\n", mTask.TaskID, mTask.fileName, w.id, w.sockName)
	args := &M2wMapArgs{FilePath: mTask.fileName, TaskID: mTask.TaskID, NReduce: master.nReduce}
	reply := &M2wMapReply{}
	result := w.call("WorkerMachine.Map", args, reply)
	if result != true {
		fmt.Println("Call Map of Worker failed")
		return
	}
	go func() {
		select {
		case <-mTask.chJobSuccess:
			// inform master that a map task was finished
			atomic.AddInt64(&master.finishedMapTasks, 1)
			// if all the map tasks are finished
			fmt.Printf("(%v/%v) [MapTask %v %v] is finished.\n", master.finishedMapTasks, len(master.mapTasks), args.TaskID, args.FilePath)
			if atomic.LoadInt64(&master.finishedMapTasks) == int64(len(master.mapTasks)) {
				close(master.chMapTasks)
			}
			return
		case <-time.After(10 * time.Second):
			fmt.Printf("[MapTask %v] is not finished within 10sec and it will be reschedule to a new worker\n", args.TaskID)
			// 10s not respond
			master.chMapTasks <- mTask
			master.zombieWorkers <- w
		}
	}()
}

func (w *worker) callReduce(rTask *reduceTask, master *Master) {
	fmt.Printf("Dispatch [ReduceTask %v] to worker [%v %v]\n", rTask.TaskID, w.id, w.sockName)
	args := M2wReduceArgs{
		FilePaths: rTask.filePaths,
		TaskID:    rTask.TaskID,
	}
	reply := &M2wReduceReply{}
	result := w.call("WorkerMachine.Reduce", args, reply)
	if result != true {
		fmt.Println("Call Reduce of Worker failed")
		return
	}
	go func() {
		select {
		case <-rTask.chJobSuccess:
			// inform master that a reduce task was finished
			atomic.AddInt64(&master.finishedReduceTasks, 1)
			fmt.Printf("(%v/%v) [ReduceTask %v] is finished.\n", master.finishedReduceTasks, master.nReduce, args.TaskID)
			// if all the reduce tasks are finished
			if atomic.LoadInt64(&master.finishedReduceTasks) == int64(master.nReduce) {
				close(master.chReduceTasks)
			}
			return
		case <-time.After(10 * time.Second):
			fmt.Printf("[ReduceTask %v] is not finished within 10sec and it will be reschedule to a new worker\n", args.TaskID)
			// 10s not respond
			master.chReduceTasks <- rTask
			master.zombieWorkers <- w
		}
	}()
}

func (w *worker) shutdown() {
	args := &M2wShutdownArgs{}
	reply := &M2wShutdownReply{}
	result := w.call("WorkerMachine.Shutdown", args, reply)
	if result {
		fmt.Printf("Worker [%v] exits...\n", reply.WorkerID)
	}
}

func (w *worker) call(rpcname string, args interface{}, reply interface{}) bool {
	err := w.client.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("call error: %v\n", err)
	return false
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:               nReduce,
		workers:               make([]*worker, 0),
		workersLock:           sync.RWMutex{},
		zombieWorkers:         make(chan *worker, 10),
		idleWorkers:           make(chan *worker, 10),
		mapTasks:              make([]*mapTask, 0),
		chMapTasks:            make(chan *mapTask, len(files)),
		reduceTasks:           make([]*reduceTask, 0),
		chReduceTasks:         make(chan *reduceTask, nReduce),
		finishedReduceTasks:   0,
		finishedMapTasks:      0,
		intermediateFilesLock: sync.RWMutex{},
		ChClose:               make(chan struct{}),
	}
	m.intermediateFiles = make([][]string, nReduce)
	for i := 0; i < nReduce; i++ {
		m.intermediateFiles[i] = make([]string, 0)
	}

	// prepare map tasks
	for i := 0; i < len(files); i++ {
		mTask := &mapTask{
			fileName:     files[i],
			chJobSuccess: make(chan struct{}),
			done:         atomic.Value{},
			TaskID:       i,
		}
		mTask.done.Store(false)
		// no race
		m.mapTasks = append(m.mapTasks, mTask)
		m.chMapTasks <- mTask
	}
	// Your code here.
	go m.revive()
	m.server()
	go m.dispatch()

	return &m
}
