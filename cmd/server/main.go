package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	const rabbitMQURL = "amqp://guest:guest@localhost:5672/"

	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to create channel:", err)
	}
	defer publishCh.Close()

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueTypeDurable,
		handleLogs(),
	)
	if err != nil {
		log.Fatal("Failed to subscribe to queue:", err)
	}
	fmt.Println("Subscribed to game log queue!")

	// Print server help
	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		firstWord := words[0]
		switch firstWord {
		case "pause":
			fmt.Println("Sending a pause message...")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)

			if err != nil {
				log.Fatal("Failed to publish message:", err)
			}

			fmt.Println("Pause message sent!")

		case "resume":
			fmt.Println("Sending a resume message...")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)

			if err != nil {
				log.Fatal("Failed to publish message:", err)
			}

			fmt.Println("Resume message sent!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command:", firstWord)
		}
	}
}
