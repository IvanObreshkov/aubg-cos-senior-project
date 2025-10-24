package server

import (
	"aubg-cos-senior-project/internal/pubsub"
	"log"
	"time"
)

/*
In this file we define all Background jobs that could run in a given Server. Each job is responsible for subscribing to
ServerShutDown events in order to exit gracefully, and prevent go routine leakage.
See: https://medium.com/@srajsonu/understanding-and-preventing-goroutine-leaks-in-go-623cac542954
*/

// TrackElectionTimeoutJob tracks the election timeout of a given server. It should be called as a goroutine.
// NOTE: The listener of electionTimeoutExpiredChan must call timer.Reset() to restart the timer, otherwise this job
// will be blocked until the timer is reset or a signal is received on stopJobCh.
func TrackElectionTimeoutJob(ctx serverCtx, electionTimeoutTimer *time.Timer, pubSub *pubsub.PubSubClient) {
	stopJobCh := make(chan *pubsub.Event[struct{}], 1)
	pubsub.Subscribe(pubSub, ServerShutDown, stopJobCh, pubsub.SubscriptionOptions{IsBlocking: false})

	log.Printf("[JOB] Started TrackElectionTimeoutJob for server %v", ctx)

	for {
		select {
		case expiredTime := <-electionTimeoutTimer.C:
			// Use serverCtx fields directly
			log.Printf("[JOB] [SERVER-%v] [TERM-%d] Election timeout expired at %v, publishing event",
				ctx.ID, ctx.Term, expiredTime.Format(time.RFC3339Nano))
			pubsub.Publish(pubSub, pubsub.NewEvent(ElectionTimeoutExpired, expiredTime))
			// Once the timer expires, the timer.C channel will NOT receive any values again, until Reset() is called
			// externally. This loop will now block until the next expiration of the timer after Reset has been called.
		case <-stopJobCh:
			// Stop the timer and exit the goroutine
			log.Printf("[JOB] Stopping TrackElectionTimeoutJob for server %v", ctx)
			electionTimeoutTimer.Stop()
			return
		}
	}
}
