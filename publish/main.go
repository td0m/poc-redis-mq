package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	for i := 0; i < 96; i++ {
		client.XAdd(ctx, &redis.XAddArgs{
			Stream: "events1",
			Values: map[string]any{
				"value": rand.Int(),
			},
			ID: "*",
		})
		time.Sleep(time.Millisecond * 10)
	}
	fmt.Println(client)
}
