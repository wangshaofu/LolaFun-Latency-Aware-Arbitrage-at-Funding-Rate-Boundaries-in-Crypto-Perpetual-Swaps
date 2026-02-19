# Binance Futures Latency Analyser

Measures WebSocket delivery latency around funding rate settlement times on Binance USD-M Futures, with the goal of identifying a viable short entry price at settlement.

---

## How It Works

1. Subscribes to the all-market mark price stream and tracks the symbol with the most negative funding rate.
2. Derives the next settlement time from the symbol's funding interval (1h, 4h, or 8h).
3. At 30 seconds before settlement, logs book ticker and aggregate trade data for 60 seconds.
4. Automatically generates a chart showing bid price, delivery latency, and trade volume per second around the settlement boundary.

---

## Setup

```ini
# config.ini
[auth]
api_key     = YOUR_API_KEY
private_key = YOUR_PRIVATE_KEY
```

```bash
pip install binance-sdk-derivatives-trading-usds-futures matplotlib
```

---

## Usage

```bash
python main.py                                          # run the scheduler
python measure_offset.py                                # check local vs Binance clock offset
python analyze_latency.py Logs/log_BTCUSDT_....txt     # rerun analysis manually
```

---

## Latency

```
latency = local_ts − E
```

`local_ts` is when the callback fired locally. `E` is when Binance stamped the message. A spike at settlement is either server-side CPU load or local event loop blocking — all blocking REST calls are offloaded via `run_in_executor` to rule out the latter.
