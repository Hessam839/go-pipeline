package ext

import streams "github.com/Hessam839/go-pipeline"

// Result is returned by a step to dispatch data to the next step or stage
//type Result struct {
//	Error error
//	// dispatch any type
//	Data interface{}
//	// dispatch key value pairs
//	KeyVal map[string]interface{}
//}
type RunFunc func(interface{}) interface{}

type Runner struct {
	RunF     RunFunc
	in       chan interface{}
	out      chan interface{}
	instance uint
}

var _ streams.Flow = (*Runner)(nil)

func NewRunner(runner RunFunc, instance uint) *Runner {
	_runner := &Runner{
		RunF:     runner,
		in:       make(chan interface{}),
		out:      make(chan interface{}),
		instance: instance,
	}
	go _runner.run()
	return _runner
}

func (r *Runner) Via(stage streams.Flow) streams.Flow {
	go r.transmit(stage)
	return stage
}

func (r *Runner) To(sink streams.Sink) {
	r.transmit(sink)
}

func (r *Runner) transmit(inlet streams.Inlet) {
	for elem := range r.out {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (r *Runner) Out() <-chan interface{} {
	return r.out
}

func (r *Runner) In() chan<- interface{} {
	return r.in
}

func (r *Runner) run() {
	sem := make(chan struct{}, r.instance)
	for elem := range r.in {
		sem <- struct{}{}
		go func(e interface{}) {
			defer func() { <-sem }()
			r.out <- r.RunF(e)
		}(elem)
	}
	for i := 0; i < int(r.instance); i++ {
		sem <- struct{}{}
	}
	close(r.out)
}

func Run(outlet streams.Outlet, inlet streams.Inlet) {
	go func() {
		for elem := range outlet.Out() {
			inlet.In() <- elem
		}
		close(inlet.In())
	}()
}

// Request is the result dispatched in a previous step.
//type DataChannel struct {
//	Error error
//	Data   interface{}
//	KeyVal map[string]interface{}
//}
//
//type Stage1 interface{
//	Exec(<-chan *DataChannel, chan<- *DataChannel)
//}
