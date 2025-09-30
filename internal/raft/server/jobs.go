package server

import (
	"aubg-cos-senior-project/internal"
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
func TrackElectionTimeoutJob(ctx ServerCtx, electionTimeoutTimer *time.Timer, pubSub *internal.PubSub) {
	stopJobCh := make(chan *internal.Event, 1)
	pubSub.Subscribe(ServerShutDown, stopJobCh, internal.SubscriptionOptions{IsBlocking: false})

	for {
		select {
		case expiredTime := <-electionTimeoutTimer.C:
			log.Printf("Election timeout expired for server %v at %v", ctx, expiredTime)
			pubSub.Publish(internal.NewEvent(ElectionTimeoutExpired, expiredTime))
			// Once the timer expires, the timer.C channel will NOT receive any values again, until Reset() is called
			// externally. This loop will now block until the next expiration of the timer after Reset has been called.
		case <-stopJobCh:
			// Stop the timer and exit the goroutine
			electionTimeoutTimer.Stop()
			return
		}
	}
}
