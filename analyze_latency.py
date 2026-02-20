import re
import sys
import os
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from collections import defaultdict

PLOT_PRE_SECONDS  = 0.3
PLOT_POST_SECONDS = 3
BUCKET_MS         = 10   # bucket width for message-rate analysis


def parse_logs(filepath):
    book_data = []
    meta      = {'symbol': None, 'funding_rate': None, 'interval': None}

    book_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - INFO - Stream message: (.*)')
    meta_pattern = re.compile(
        r'Starting latency logging for (\w+) \| funding_rate=([\-0-9\.]+) \| interval=(\w+)h'
    )
    kv_pattern   = re.compile(r"(\w+)=['\"]?([a-zA-Z0-9\.]+)['\"]?")

    print(f"Reading {filepath}...")
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if meta['symbol'] is None:
                    m = meta_pattern.search(line)
                    if m:
                        meta['symbol']       = m.group(1)
                        meta['funding_rate'] = float(m.group(2))
                        meta['interval']     = m.group(3)

                match = book_pattern.search(line)
                if match:
                    params = dict(kv_pattern.findall(match.group(2)))
                    if params.get('e') == 'bookTicker' and 'E' in params and 'b' in params and 'a' in params:
                        try:
                            log_dt = datetime.datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S,%f')
                            book_data.append({
                                'local_ts':      log_dt.timestamp() * 1000,
                                'E':             int(params['E']),
                                'b':             float(params['b']),
                                'a':             float(params['a']),
                                'readable_time': match.group(1),
                            })
                        except ValueError:
                            continue
    except FileNotFoundError:
        print(f"Error: File {filepath} not found.")

    return book_data, meta


def _find_clock_boundaries(timestamps):
    t_start = timestamps[0]
    t_end   = timestamps[-1]
    first   = t_start.replace(second=0, microsecond=0)
    if first < t_start:
        first += datetime.timedelta(minutes=1)
    boundaries, t = [], first
    while t <= t_end:
        boundaries.append(t)
        t += datetime.timedelta(minutes=1)
    return boundaries


def _first_index_at_or_after(timestamps, target):
    for i, ts in enumerate(timestamps):
        if ts >= target:
            return i
    return None


# ---------------------------------------------------------------------------
# Message-rate analysis
# ---------------------------------------------------------------------------

def _bucket_key(ts_ms: float, bucket_ms: int = BUCKET_MS) -> float:
    """Floor a millisecond timestamp to the nearest bucket boundary."""
    return float(int(ts_ms // bucket_ms) * bucket_ms)


def compute_message_rate(book_data, bucket_ms: int = BUCKET_MS):
    """
    Count messages per bucket_ms window using the *event* timestamp (E).
    Returns parallel lists suitable for a bar chart:
        bucket_times  – datetime of each bucket start
        counts        – number of messages in that bucket
    """
    counts: dict[float, int] = defaultdict(int)
    for entry in book_data:
        counts[_bucket_key(entry['E'], bucket_ms)] += 1

    sorted_keys = sorted(counts)
    bucket_times = [datetime.datetime.fromtimestamp(k / 1000) for k in sorted_keys]
    bucket_counts = [counts[k] for k in sorted_keys]
    return bucket_times, bucket_counts


def print_message_rate_stats(book_data, latencies, timestamps, bucket_ms: int = BUCKET_MS):
    """
    Print a table of every bucket, its message count, and the max / mean latency
    observed inside it.  Highlights buckets where both count and latency are elevated.
    """
    from collections import defaultdict

    bucket_latencies: dict[float, list[float]] = defaultdict(list)
    for entry, lat in zip(book_data, latencies):
        bucket_latencies[_bucket_key(entry['E'], bucket_ms)].append(lat)

    sorted_keys = sorted(bucket_latencies)

    # Global baseline (median latency across all buckets' means)
    all_means = [np.mean(v) for v in bucket_latencies.values()]
    baseline  = float(np.median(all_means))
    global_count_mean = np.mean([len(v) for v in bucket_latencies.values()])

    print("\n" + "=" * 70)
    print(f"Message-rate analysis  (bucket = {bucket_ms} ms,  baseline latency ≈ {baseline:.1f} ms)")
    print(f"{'Bucket':>23}  {'Count':>5}  {'MaxLat':>8}  {'MeanLat':>8}  {'Flag'}")
    print("-" * 70)

    for k in sorted_keys:
        lats   = bucket_latencies[k]
        count  = len(lats)
        max_l  = max(lats)
        mean_l = np.mean(lats)
        dt_str = datetime.datetime.fromtimestamp(k / 1000).strftime('%H:%M:%S.%f')[:-3]

        # Flag buckets where latency is elevated (>2× baseline) AND message count is also elevated
        high_lat   = mean_l > baseline * 1.2
        high_count = count  > global_count_mean * 2
        flag = ""
        if high_lat and high_count:
            flag = "⚠ HIGH-COUNT + HIGH-LAT"
        elif high_lat:
            flag = "  HIGH-LAT only"
        elif high_count:
            flag = "  HIGH-COUNT only"

        print(f"  {dt_str:>21}  {count:>5}  {max_l:>8.1f}  {mean_l:>8.1f}  {flag}")

    # --- Correlation summary ---
    counts_arr   = np.array([len(bucket_latencies[k]) for k in sorted_keys], dtype=float)
    mean_lats    = np.array([np.mean(bucket_latencies[k]) for k in sorted_keys], dtype=float)
    corr         = float(np.corrcoef(counts_arr, mean_lats)[0, 1]) if len(counts_arr) > 1 else float('nan')

    print("-" * 70)
    print(f"Pearson correlation (msg count vs mean latency): {corr:+.5f}")
    if abs(corr) > 0.6:
        print("  → Strong correlation: message volume likely contributes to latency.")
    elif abs(corr) > 0.3:
        print("  → Moderate correlation: possible relationship, inspect flagged buckets.")
    else:
        print("  → Weak correlation: latency spikes appear independent of message rate.")
    print("=" * 70 + "\n")


# ---------------------------------------------------------------------------
# Plot
# ---------------------------------------------------------------------------

def analyze_and_plot(book_data, output_plot_path=None, meta=None):
    if not book_data:
        print("No valid 'bookTicker' data found in logs.")
        return

    timestamps, latencies, bids, asks = [], [], [], []

    for i, entry in enumerate(book_data):
        latency = entry['local_ts'] - entry['E']
        timestamps.append(datetime.datetime.fromtimestamp(entry['E'] / 1000))
        latencies.append(latency)
        bids.append(entry['b'])
        asks.append(entry['a'])

    # --- x-axis window (computed before max latency search) ---
    clock_boundaries = _find_clock_boundaries(timestamps)
    if clock_boundaries:
        anchor = clock_boundaries[0]
        x_min  = anchor - datetime.timedelta(seconds=PLOT_PRE_SECONDS)
        x_max  = anchor + datetime.timedelta(seconds=PLOT_POST_SECONDS)
    else:
        x_min, x_max = timestamps[0], timestamps[-1]

    # --- Max latency within the plot window only ---
    max_latency, max_latency_idx = -float('inf'), -1
    for i, (ts, lat) in enumerate(zip(timestamps, latencies)):
        if x_min <= ts <= x_max and lat > max_latency:
            max_latency, max_latency_idx = lat, i

    # --- Stats ---
    print("-" * 50)
    if max_latency_idx != -1:
        e = book_data[max_latency_idx]
        print(f"Max Latency (Local - Event): {max_latency:.2f} ms")
        print(f"  Local Log Time : {e['readable_time']}")
        print(f"  Event Time (E) : {e['E']}")
    print("-" * 50)

    # --- Message-rate stats (console) ---
    print_message_rate_stats(book_data, latencies, timestamps)

    # --- Message-rate data for plot ---
    bucket_times, bucket_counts = compute_message_rate(book_data)
    bucket_width_dt = datetime.timedelta(milliseconds=BUCKET_MS * 0.85)  # slight gap between bars

    # --- Plot (3 subplots) ---
    plt.style.use('ggplot')
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True, figsize=(14, 13),
                                         gridspec_kw={'height_ratios': [2, 2, 1]})

    # Figure-level title with funding rate info
    has_suptitle = False
    if meta and meta.get('funding_rate') is not None:
        rate     = meta['funding_rate']
        symbol   = meta.get('symbol', '?')
        interval = meta.get('interval', '?')
        color    = 'red' if rate < 0 else 'green'
        fig.suptitle(
            f"{symbol}  |  Funding Rate: {rate:.8f}  |  Interval: {interval}h",
            fontsize=12, fontweight='bold', color=color, y=0.98
        )
        has_suptitle = True

    # Subplot 1: Bid + Ask price
    ax1.plot(timestamps, bids, label='Bid', color='green', linewidth=1.5, alpha=0.8)
    ax1.plot(timestamps, asks, label='Ask', color='red',   linewidth=1.5, alpha=0.8)
    ax1.set_ylabel('Price')
    ax1.set_title('Bid / Ask Price')
    ax1.grid(True, linestyle=':', alpha=0.6)

    # Subplot 2: Latency
    ax2.plot(timestamps, latencies, label='Latency (Local − Event)', color='purple', linewidth=1.5)
    ax2.set_ylabel('Latency (ms)', color='purple')
    ax2.tick_params(axis='y', labelcolor='purple')
    ax2.set_title('Latency (Local − Event Time)')
    ax2.grid(True, linestyle=':', alpha=0.6)

    # Subplot 3: Messages per 100ms bucket
    bar_colors = ['crimson' if c > np.mean(bucket_counts) * 1.5 else 'steelblue'
                  for c in bucket_counts]
    ax3.bar(bucket_times, bucket_counts, width=bucket_width_dt,
            color=bar_colors, alpha=0.8, label=f'Msgs per {BUCKET_MS}ms')
    ax3.axhline(y=np.mean(bucket_counts), color='gray', linewidth=1.0,
                linestyle='--', label=f'Mean ({np.mean(bucket_counts):.1f})')
    ax3.set_ylabel('Msg count')
    ax3.set_title(f'Messages per {BUCKET_MS}ms  (red = >1.5× mean)')
    ax3.legend(loc='upper left', fontsize=8)
    ax3.grid(True, linestyle=':', alpha=0.6)

    # Orange vertical line at max latency + bid horizontal after it
    if max_latency_idx != -1:
        max_local_dt = datetime.datetime.fromtimestamp(book_data[max_latency_idx]['local_ts'] / 1000)
        for ax in (ax1, ax2, ax3):
            ax.axvline(x=max_local_dt, color='orange', linewidth=1.5, linestyle='--', label='Max Latency')
        ax2.annotate(
            f'Max: {max_latency:.2f}ms',
            xy=(timestamps[max_latency_idx], latencies[max_latency_idx]),
            xytext=(-10, 20), textcoords='offset points',
            arrowprops=dict(arrowstyle="->", color='purple'),
            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="purple", alpha=0.8),
        )
        post_max_idx = max_latency_idx + 1
        if post_max_idx < len(timestamps):
            post_max_bid = bids[post_max_idx]
            hline_start  = max(timestamps[post_max_idx], x_min)
            hline_end    = min(x_max, x_max)
            ax1.hlines(y=post_max_bid, xmin=hline_start, xmax=hline_end,
                       color='orange', linewidth=1.2, linestyle='--', alpha=0.9,
                       label=f'Bid after Max Lat: {post_max_bid:.5f}')
            ax1.text(hline_start, post_max_bid, f'{post_max_bid:.5f}',
                     fontsize=7, color='orange', verticalalignment='bottom')

    # Clock minute boundaries + bid horizontal at first point after each
    for clock_minute in clock_boundaries:
        for ax in (ax1, ax2, ax3):
            ax.axvline(x=clock_minute, color='blue', linewidth=1.0, linestyle=':', alpha=0.7)

        # Vertical line at 200ms after the clock boundary
        clock_plus_200ms = clock_minute + datetime.timedelta(milliseconds=200)
        for ax in (ax1, ax2, ax3):
            ax.axvline(x=clock_plus_200ms, color='lime', linewidth=1.0, linestyle='--', alpha=0.8)
        ax2.text(clock_plus_200ms, ax2.get_ylim()[0], '+200ms',
                 rotation=90, fontsize=7, color='lime', verticalalignment='bottom')

        # Horizontal line on bid/ask at the first point at or after +200ms
        j200 = _first_index_at_or_after(timestamps, clock_plus_200ms)
        if j200 is not None:
            bid_200 = bids[j200]
            next_minute = clock_minute + datetime.timedelta(minutes=1)
            hline_start = max(clock_plus_200ms, x_min)
            hline_end   = min(next_minute, x_max)
            ax1.hlines(y=bid_200, xmin=hline_start, xmax=hline_end,
                       color='lime', linewidth=1.2, linestyle='--', alpha=0.9,
                       label='+200ms Bid' if clock_minute == clock_boundaries[0] else '')
            ax1.text(hline_start, bid_200, f'{bid_200:.5f}',
                     fontsize=7, color='lime', verticalalignment='bottom')

        j = _first_index_at_or_after(timestamps, clock_minute)
        if j is not None:
            entry_bid   = bids[j]
            next_minute = clock_minute + datetime.timedelta(minutes=1)
            hline_start = max(clock_minute, x_min)
            hline_end   = min(next_minute, x_max)
            ax1.hlines(y=entry_bid, xmin=hline_start, xmax=hline_end,
                       color='blue', linewidth=1.2, linestyle='--', alpha=0.9,
                       label='Bid at Settlement' if clock_minute == clock_boundaries[0] else '')
            ax1.text(hline_start, entry_bid, f'{entry_bid:.5f}',
                     fontsize=7, color='blue', verticalalignment='bottom')

    for ax in (ax1, ax2, ax3):
        ax.set_xlim(x_min, x_max)
        ax.legend(loc='upper left', fontsize=8)

    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.xlabel('Event Time')
    if has_suptitle:
        plt.tight_layout(rect=[0, 0, 1, 0.96])
    else:
        plt.tight_layout()

    if output_plot_path:
        os.makedirs(os.path.dirname(output_plot_path), exist_ok=True)
        plt.savefig(output_plot_path)
        print(f"Plot saved to {output_plot_path}")
        plt.close()
    else:
        plt.show()


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else './Logs'

    if os.path.isfile(target):
        # Called with a single log file (e.g. from streams.py subprocess)
        book_data, meta = parse_logs(target)
        plot_filename = os.path.basename(target).replace('.txt', '.png')
        plot_path = os.path.join('Plots', plot_filename)
        analyze_and_plot(book_data, plot_path, meta=meta)
    else:
        # Called with a directory — process all .txt files inside it
        log_files = sorted(f for f in os.listdir(target) if f.endswith('.txt'))

        if not log_files:
            print(f"No .txt log files found in {target}")
            sys.exit(0)

        for filename in log_files:
            filepath = os.path.join(target, filename)
            book_data, meta = parse_logs(filepath)
            plot_filename = filename.replace('.txt', '.png')
            plot_path = os.path.join('Plots', plot_filename)
            analyze_and_plot(book_data, plot_path, meta=meta)
