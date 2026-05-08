# kiharo

Adaptive hedged requests for Go. Reduces tail latency without external dependencies.

[![Go Reference](https://pkg.go.dev/badge/github.com/BawNer/kiharo.svg)](https://pkg.go.dev/github.com/BawNer/kiharo)
[![Go Report Card](https://goreportcard.com/badge/github.com/BawNer/kiharo)](https://goreportcard.com/report/github.com/BawNer/kiharo)

## What it does

When a request is slow, `kiharo` fires additional parallel attempts after a delay derived from real latency statistics. The first successful response wins; the rest are cancelled via context. P99 spikes shrink, your zero-config setup stays zero-config.

## Why another hedging library

Most hedging libraries make you pick a delay by hand. Pick it too low and you double your load. Pick it too high and the hedge never helps. The "right" delay depends on your dependency's actual P75 latency right now — which changes throughout the day.

`kiharo` measures it. The library keeps a sliding window of recent successful first-attempt latencies per key and uses a configurable percentile of that window as the hedge delay. No StatsD, no Prometheus required, no external service. Just a fixed-size ring buffer in memory.

## Install

```bash
go get github.com/BawNer/kiharo
```

Requires Go 1.21+.

## Usage

```go
package main

import (
    "context"
    "time"

    "github.com/BawNer/kiharo"
)

func main() {
    hedger := kiharo.New()

    hedger.Register("get-user", kiharo.RegisterConfig{
        MaxCalls:     2,
        Window:       kiharo.WindowSmall,   // 100 samples
        Percentile:   kiharo.P75,           // hedge when first call is slower than 75% of history
        MinDelay:     5 * time.Millisecond,
        MaxDelay:     500 * time.Millisecond,
        DefaultDelay: 20 * time.Millisecond, // used until the window fills
    })

    user, err := kiharo.Do(ctx, hedger, "get-user", func(ctx context.Context) (User, error) {
        return client.GetUser(ctx, id)
    })
}
```

That's the whole API. One `Hedger` per application, register your keys at startup, call `Do` from anywhere.

## How the adaptive delay works
