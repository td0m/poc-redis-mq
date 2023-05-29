package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	var group string
	flag.StringVar(&group, "group", "logging", "set xgroup")
	flag.Parse()

	stream := "events1"
	consumer := "c1"

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()

	client.XGroupCreate(ctx, stream, group, "0").Result()

	go func() {
		for {
			msges, start, err := client.XAutoClaimJustID(ctx, &redis.XAutoClaimArgs{
				Stream:   stream,
				Group:    group,
				Consumer: consumer,
				MinIdle:  time.Second * 10,
				Count:    10,
				Start:    "0-0",
			}).Result()
			check(err)
			if len(msges) > 0 {
				fmt.Println(msges, len(msges), start)
				client.XAck(ctx, stream, group, msges...)
				continue
			}
			time.Sleep(time.Second * 10)
		}
	}()

	for {
		fmt.Print("Scanning... ")
		streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Streams:  []string{stream, ">"},
			Group:    group,
			Consumer: consumer,
			Count:    10,
			Block:    time.Second,
			// NoAck:    true,
		}).Result()
		if errors.Is(err, redis.Nil) {
			fmt.Println("no items")
			time.Sleep(time.Second * 3)
			continue
		}
		check(err)
		fmt.Println(len(streams[0].Messages))
		for _, msg := range streams[0].Messages {
			if rand.Float32() < 0.3 {
				client.XAck(ctx, stream, group, msg.ID)
			}
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
