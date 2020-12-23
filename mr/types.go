package mr

const (
	MapPhase = iota
	ReducePhase
)

type Task struct {
	Id int
	Key string
	Status int
	Complete chan struct{}
}

const (
	TaskIdle = iota
	TaskInProgress
	TaskComplete
)

const (
	Idle = iota
	Working
	Initial
	Unknown
)

const (
	MapTask = iota
	ReduceTask
)