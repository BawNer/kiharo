# kiharo

Hedged requests for Go with adaptive delay. No external dependencies.

[![CI](https://github.com/BawNer/kiharo/actions/workflows/ci.yml/badge.svg)](https://github.com/BawNer/kiharo/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/BawNer/kiharo.svg)](https://pkg.go.dev/github.com/BawNer/kiharo)
[![Go Report Card](https://goreportcard.com/badge/github.com/BawNer/kiharo)](https://goreportcard.com/report/github.com/BawNer/kiharo)

## What

If a call takes too long, kiharo fires another one (or two) in parallel after some delay. Whoever finishes first wins, the rest get cancelled via context. Helps with tail latency.

The delay is the part most libraries get wrong, so let's talk about it.

## Why I wrote this

I needed hedging in a service and looked at what was already out there. Almost everything wants you to hardcode the delay. Pick 10ms and you'll hedge basically every request (load goes up 2x for nothing). Pick 200ms and the hedge fires after the user is gone. The "right" number is something like P75 of your real latency, which drifts during the day depending on how the upstream feels.

So measure it. kiharo keeps a small ring buffer of recent first-attempt latencies per key and takes a percentile from it. That's the delay. No StatsD, no Prom, just a slice in memory.

## Install

```bash
go get github.com/BawNer/kiharo
```

Go 1.22+.

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
        Percentile:   kiharo.P75,
        MinDelay:     5 * time.Millisecond,
        MaxDelay:     500 * time.Millisecond,
        DefaultDelay: 20 * time.Millisecond, // until the window fills
    })

    user, err := kiharo.Do(ctx, hedger, "get-user", func(ctx context.Context) (User, error) {
        return client.GetUser(ctx, id)
    })
}
```

One Hedger per app, register your keys at startup, call `Do` from wherever.

## The delay

```
window not full → DefaultDelay
window full     → percentile of the window
result          → clamped to [MinDelay, MaxDelay]
```

Only first attempts feed the window. Hedged ones are ignored on purpose, otherwise you end up with a loop: smaller delay → more hedges → percentile drops → smaller delay → ...

## Config

`RegisterConfig` doesn't accept arbitrary values. The ones that mostly hurt are not allowed:

| Field | Allowed | Default |
|---|---|---|
| `MaxCalls` | 1, 2, 3 | required |
| `Window` | `WindowSmall` (100), `WindowMedium` (500), `WindowLarge` (1000) | required |
| `Percentile` | `P75`, `P90`, `P95` | required |
| `MinDelay` | `>= 0` | required |
| `MaxDelay` | `> 0`, `>= MinDelay` | required |
| `DefaultDelay` | `> 0` | required |
| `AttemptTimeout` | `>= 0`, 0 means no limit | 0 |
| `IsRetryable` | `func(error) bool`, nil = retry everything | nil |

`MaxCalls=10` is just DoS on yourself. `P50` means you hedge half your traffic, which is silly. `Window=10` is noise, not stats. So those aren't options.

## Per-attempt timeout

`AttemptTimeout` caps wall time for one attempt. When it trips, kiharo cancels that attempt's ctx and treats it like a transient failure, the next hedge keeps going:

```go
hedger.Register("flaky-upstream", kiharo.RegisterConfig{
    MaxCalls:       3,
    AttemptTimeout: 200 * time.Millisecond,
    // ...
})
```

If every attempt times out, you get `kiharo.ErrAttemptTimeout`. It wraps `context.DeadlineExceeded`, so either check works:

```go
errors.Is(err, kiharo.ErrAttemptTimeout)        // specifically the attempt timeout
errors.Is(err, context.DeadlineExceeded)        // any deadline
```

You can also wrap the whole `Do` in `context.WithTimeout` for the total budget. Whichever fires first wins, they compose fine.

## Retryable errors

Hedging a 404 is pointless, the answer isn't going to change. `IsRetryable` lets you short-circuit:

```go
hedger.Register("userapi.get", kiharo.RegisterConfig{
    MaxCalls: 2,
    // ...
    IsRetryable: func(err error) bool {
        var apiErr *userapi.Error
        if errors.As(err, &apiErr) {
            return apiErr.Status >= 500
        }
        return true // network/timeouts: try again
    },
})
```

If `IsRetryable` returns false, `Do` returns immediately, in-flight attempts get cancelled.

One exception: `AttemptTimeout` failures are always retryable, even if your `IsRetryable` would say no. Reason: a timeout means the attempt got stuck, it's not a real answer from the server.

## Memory

`Window * 8 bytes` per key. So `WindowLarge` is 8 KB. Bounded, predictable, nothing to evict.

## Metrics

Plug in your own. Implement `MetricsRecorder`:

```go
type MetricsRecorder interface {
    RecordRequest(hedged bool)
    RecordResponse(hedged bool, err error, duration time.Duration)
    RecordHedgeWin()
}

hedger := kiharo.New(kiharo.WithMetrics(myRecorder))
```

`RecordHedgeWin` is the useful one. It's how often the hedge actually saved a slow call. If you see ~0, your percentile is too high. If you see ~half your traffic, it's too low.

## Notes

- `Do` is a top-level function, not a method. Go still doesn't allow type params on methods.
- `Register` panics on bad config or a duplicate key. That's intentional, startup-time bugs should be loud.
- On full failure you get the first error back, not a wrapped multi-error.
- Context cancellation: child attempts inherit from `Do`'s ctx, the winner cancels the others, parent cancel tears everything down.
- `AttemptTimeout` is infra-level (got stuck), `IsRetryable` is domain-level (server said no). They're separate fields on purpose.

## Where to put `Register`

One Hedger shared across packages is fine. I usually put the `Register` call in the constructor of whatever client knows the upstream:

```go
func NewUserClient(http *http.Client, hedger *kiharo.Hedger) *UserClient {
    hedger.Register("userapi.get", kiharo.RegisterConfig{
        MaxCalls:       2,
        Window:         kiharo.WindowSmall,
        Percentile:     kiharo.P75,
        MinDelay:       5 * time.Millisecond,
        MaxDelay:       500 * time.Millisecond,
        DefaultDelay:   20 * time.Millisecond,
        AttemptTimeout: 300 * time.Millisecond,
        IsRetryable:    isHTTPRetryable,
    })
    return &UserClient{http: http, hedger: hedger}
}
```

`main` stays small: build a Hedger, pass it around.

## Related

A couple of other small libs I maintain in a similar style:

- [`kahora`](https://github.com/BawNer/kahora) — sharded cache with gradual map shrink
- [`haroku`](https://github.com/BawNer/haroku) — graceful shutdown, register from anywhere

## License

MIT