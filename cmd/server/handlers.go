package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handleLogs() func(routing.GameLog) pubsub.AckType {
	return func(gamelog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")

		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Println("Error writing log:", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
