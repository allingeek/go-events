package main

import (
	"context"
	"log"
	"os"
	"time"

	events "github.com/allingeek/go-events"
)

func main() {
	r := events.NewRegistry(time.Minute, log.New(os.Stderr, ``, log.LUTC))
	ec := events.EventsConfig{
		Registry: r,
		Tags:     map[string]string{`SomeKey`: `SomeValue`},
	}
	ctx := events.NewContext(context.Background(), ec)

	r.Subscribe(`stdout`, events.PublishLog())

	aTest(ctx)
	<-time.After(time.Second)

}

func aTest(ctx context.Context) {
	c := events.NewCounter(`demoCounter`)
	g := events.NewCounter(`demoGrowthCounter`)
	t := events.NewTimer(`demoTimer`)
	t.Start()
	defer events.Write(ctx, c, t, g)
	for i := 0; i < 100; i++ {
		c.Add(1)
		g.Add(float64(i))
		<-time.After(100 * time.Millisecond)
	}
	t.Stop()
}
