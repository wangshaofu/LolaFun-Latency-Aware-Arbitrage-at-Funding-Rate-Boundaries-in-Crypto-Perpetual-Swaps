import re
import sys
import os
import datetime
import collections
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

PLOT_PRE_SECONDS  = 3
PLOT_POST_SECONDS = 10


def parse_logs(filepath):
    book_data  = []
    agg_trades = []

    book_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - INFO - Stream message: (.*)')
    agg_pattern  = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - INFO - Agg trade: (.*)')
    kv_pattern   = re.compile(r"(\w+)=['\"]?([a-zA-Z0-9\.]+)['\"]?")

    print(f"Reading {filepath}...")
    try:
        with open(filepath, 'r') as f:
            for line in f:
                # Book ticker
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

                # Agg trade
                match = agg_pattern.search(line)
                if match:
                    params = dict(kv_pattern.findall(match.group(2)))
                    if 'T' in params and 'q' in params:
                        try:
                            agg_trades.append({
                                'T': int(params['T']),
                                'q': float(params['q']),
                            })
                        except ValueError:
                            continue

    except FileNotFoundError:
        print(f"Error: File {filepath} not found.")

    return book_data, agg_trades


def _bucket_volume_by_second(agg_trades, x_min, x_max):
    """Sum trade quantity per second within the plot window."""
    buckets = collections.defaultdict(float)
    for trade in agg_trades:
        ts = datetime.datetime.fromtimestamp(trade['T'] / 1000)
        second = ts.replace(microsecond=0)
        if x_min <= second <= x_max:
            buckets[second] += trade['q']
    if not buckets:
        return [], []
    seconds = sorted(buckets)
    volumes = [buckets[s] for s in seconds]
    return seconds, volumes


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


def analyze_and_plot(book_data, agg_trades, output_plot_path=None):
    if not book_data:
        print("No valid 'bookTicker' data found in logs.")
        return

    timestamps, latencies, bids = [], [], []
    max_latency, max_latency_idx = -float('inf'), -1

    for i, entry in enumerate(book_data):
        latency = entry['local_ts'] - entry['E']
        if latency > max_latency:
            max_latency, max_latency_idx = latency, i
        timestamps.append(datetime.datetime.fromtimestamp(entry['E'] / 1000))
        latencies.append(latency)
        bids.append(entry['b'])

    # --- Stats ---
    print("-" * 50)
    if max_latency_idx != -1:
        e = book_data[max_latency_idx]
        print(f"Max Latency (Local - Event): {max_latency:.2f} ms")
        print(f"  Local Log Time : {e['readable_time']}")
        print(f"  Event Time (E) : {e['E']}")
    print("-" * 50)

    # --- x-axis window ---
    clock_boundaries = _find_clock_boundaries(timestamps)
    if clock_boundaries:
        anchor = clock_boundaries[0]
        x_min  = anchor - datetime.timedelta(seconds=PLOT_PRE_SECONDS)
        x_max  = anchor + datetime.timedelta(seconds=PLOT_POST_SECONDS)
    else:
        x_min, x_max = timestamps[0], timestamps[-1]

    # --- Volume buckets ---
    vol_seconds, vol_values = _bucket_volume_by_second(agg_trades, x_min, x_max)
    has_volume = bool(vol_seconds)

    # --- Plot ---
    plt.style.use('ggplot')
    nrows = 3 if has_volume else 2
    fig, axes = plt.subplots(nrows, 1, sharex=True, figsize=(14, 5 * nrows))
    ax1, ax2 = axes[0], axes[1]
    ax3 = axes[2] if has_volume else None

    # Subplot 1: Bid price
    ax1.plot(timestamps, bids, label='Bid Price', color='green', linewidth=1.5, alpha=0.8)
    ax1.set_ylabel('Price')
    ax1.set_title('Bid Price')
    ax1.grid(True, linestyle=':', alpha=0.6)

    # Subplot 2: Latency
    ax2.plot(timestamps, latencies, label='Latency (Local - Event)', color='purple', linewidth=1.5)
    ax2.set_ylabel('Latency (ms)', color='purple')
    ax2.tick_params(axis='y', labelcolor='purple')
    ax2.set_title('Latency (Local - Event Time)')
    ax2.grid(True, linestyle=':', alpha=0.6)

    # Subplot 3: Volume bar chart
    if ax3:
        ax3.bar(vol_seconds, vol_values, width=0.8 / 86400, color='steelblue', alpha=0.8, label='Volume (qty/s)')
        ax3.set_ylabel('Volume (qty)')
        ax3.set_title('Trade Volume per Second')
        ax3.grid(True, linestyle=':', alpha=0.6, axis='y')

    # Orange vertical line at max latency + bid horizontal after it
    if max_latency_idx != -1:
        max_local_dt = datetime.datetime.fromtimestamp(book_data[max_latency_idx]['local_ts'] / 1000)
        for ax in axes:
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
            ax1.hlines(y=post_max_bid, xmin=timestamps[post_max_idx], xmax=x_max,
                       color='orange', linewidth=1.2, linestyle='--', alpha=0.9,
                       label=f'Bid after Max Lat: {post_max_bid:.4f}')
            ax1.text(timestamps[post_max_idx], post_max_bid, f'{post_max_bid:.4f}',
                     fontsize=7, color='orange', verticalalignment='bottom')

    # Clock minute boundaries + bid horizontal at first point after each
    for clock_minute in clock_boundaries:
        for ax in axes:
            ax.axvline(x=clock_minute, color='gray', linewidth=1.0, linestyle=':', alpha=0.7)
        ax2.text(clock_minute, ax2.get_ylim()[0], clock_minute.strftime('%H:%M'),
                 rotation=90, fontsize=7, color='gray', verticalalignment='bottom')

        j = _first_index_at_or_after(timestamps, clock_minute)
        if j is not None:
            entry_bid   = bids[j]
            next_minute = clock_minute + datetime.timedelta(minutes=1)
            ax1.hlines(y=entry_bid, xmin=clock_minute, xmax=min(next_minute, timestamps[-1]),
                       color='blue', linewidth=1.2, linestyle='--', alpha=0.9,
                       label='Bid at Settlement' if clock_minute == clock_boundaries[0] else '')
            ax1.text(clock_minute, entry_bid, f'{entry_bid:.4f}',
                     fontsize=7, color='blue', verticalalignment='bottom')

    for ax in axes:
        ax.set_xlim(x_min, x_max)
        ax.legend(loc='upper left')

    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.xlabel('Event Time')
    plt.tight_layout()

    if output_plot_path:
        os.makedirs(os.path.dirname(output_plot_path), exist_ok=True)
        plt.savefig(output_plot_path)
        print(f"Plot saved to {output_plot_path}")
        plt.close()
    else:
        plt.show()


if __name__ == "__main__":
    filepath = sys.argv[1] if len(sys.argv) > 1 else './Logs/log_INJUSDT_20260219_155930.txt'

    book_data, agg_trades = parse_logs(filepath)
    plot_filename = os.path.basename(filepath).replace('.txt', '.png')
    plot_path = os.path.join('Plots', plot_filename)
    analyze_and_plot(book_data, agg_trades, plot_path)
