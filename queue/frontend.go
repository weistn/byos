package queue

// The Frontend is the API of the queueing system.
// It uses workers to carry out its jobs.
type Frontend struct {
	log *commitLog
}
