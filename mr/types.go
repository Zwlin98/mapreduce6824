package mr

const (
	MapPhase = iota
	ReducePhase
	Finish
)

type Task struct {
	Id       int
	Key      string
	Status   int
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

func WorkerStatus(status int) string {
	switch status {
	case Idle:
		return "Idle"
	case Working:
		return "Working"
	case Initial:
		return "Initial"
	case Unknown:
		return "Unknown"
	}
	return ""
}
