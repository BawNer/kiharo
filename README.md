# kiharo

Adaptive hedged requests for Go. Reduces tail latency without external dependencies.

[![CI](https://github.com/BawNer/kiharo/actions/workflows/ci.yml/badge.svg)](https://github.com/BawNer/kiharo/actions/workflows/ci.yml)
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

Requires Go 1.26+.

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

Calls 1..N-1   → window not full → use DefaultDelay
Calls N+       → window full → use Percentile of window
→ clamped to [MinDelay, MaxDelay]

Only successful first attempts feed the window. Hedged latencies are excluded — otherwise you'd get a feedback loop where smaller delays produce more hedge samples and push the percentile down further.

## Configuration

`RegisterConfig` is opinionated by design. Bad values that hurt more than help are not exposed:

| Field | Allowed values |
|---|---|
| `MaxCalls` | 1, 2, or 3 |
| `Window` | `WindowSmall` (100), `WindowMedium` (500), `WindowLarge` (1000) |
| `Percentile` | `P75`, `P90`, `P95` |

`MaxCalls=10` is a self-DoS. `Percentile=P50` means hedging half your traffic. `Window=10` gives you noise. The library makes those choices for you.

## Memory

`Window × 8 bytes` per registered key. `WindowLarge` = 8 KB. Predictable, bounded, no eviction logic to misconfigure.

## Metrics

Bring your own. Implement `MetricsRecorder`:

```go
type MetricsRecorder interface {
    RecordRequest(hedged bool)
    RecordResponse(hedged bool, err error, duration time.Duration)
    RecordHedgeWin()
}

hedger := kiharo.New(kiharo.WithMetrics(myRecorder))
```

`RecordHedgeWin` is the one to watch — it tells you how often hedging actually rescued a slow call. If it's near zero, your `Percentile` is too high. If it's near 50%, it's too low.

## Design notes

- **Top-level `Do`, not a method.** Go doesn't allow type parameters on methods yet. Pass the `*Hedger` explicitly.
- **Panic on misconfiguration.** `Register` panics on invalid config or duplicate keys. Startup-time bugs should fail loud, not lurk.
- **First successful attempt wins, first error returned.** When all attempts fail, you get the first error that came back, not a wrapped multi-error.
- **Context cancellation propagates.** Children inherit, winner cancels siblings, parent cancellation tears everything down.

## Recipe: register near the domain

The library is happy with one `Hedger` shared across packages. Register where the domain knowledge lives — usually in the constructor of the client that knows what its dependency looks like:

```go
func NewUserClient(http *http.Client, hedger *kiharo.Hedger) *UserClient {
    hedger.Register("userapi.get", kiharo.RegisterConfig{
        MaxCalls:     2,
        Window:       kiharo.WindowSmall,
        Percentile:   kiharo.P75,
        // ...
    })
    return &UserClient{http: http, hedger: hedger}
}
```

The `main` stays thin: build one `Hedger`, hand it out.

## Ecosystem

Part of a small set of focused Go libraries with the same philosophy — simple API, no magic, bring your own metrics:

- [`kahora`](https://github.com/BawNer/kahora) — sharded cache with gradual map shrink
- [`haroku`](https://github.com/BawNer/haroku) — graceful shutdown, register anywhere
- `kiharo` — adaptive hedged requests

## Status

[Pre-1.0 / experimental / production / whatever fits]. API may change before v1.0.

## License

MIT
