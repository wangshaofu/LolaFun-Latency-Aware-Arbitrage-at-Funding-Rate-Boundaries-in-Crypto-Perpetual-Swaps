import configparser
import time
from binance_common.configuration import ConfigurationRestAPI
from binance_common.constants import DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import DerivativesTradingUsdsFutures

try:
    config = configparser.ConfigParser()
    config.read('config.ini')
    api_key = config.get("paper_auth", 'api_key')
    private_key = config.get("paper_auth", 'private_key')

    rest_config = ConfigurationRestAPI(
        api_key=api_key,
        private_key=private_key,
        base_path=DERIVATIVES_TRADING_USDS_FUTURES_REST_API_PROD_URL
    )
    client = DerivativesTradingUsdsFutures(config_rest_api=rest_config)

    best_offset = None
    best_rtt = None
    rtts = []
    offsets = []

    print("Sampling Binance server time (20 requests)...")

    # Warmup request to establish TCP/TLS connection before sampling
    client.rest_api.check_server_time()

    for i in range(20):
        t0 = int(time.time() * 1000)
        resp = client.rest_api.check_server_time()
        t1 = int(time.time() * 1000)
        server_time = resp.data().server_time
        rtt = t1 - t0
        midpoint = (t0 + t1) // 2
        offset = server_time - midpoint
        rtts.append(rtt)
        offsets.append(offset)
        if best_rtt is None or rtt < best_rtt:
            best_rtt = rtt
            best_offset = offset
        print(f"  [{i+1:2}] RTT={rtt}ms  offset={offset:+}ms")
        time.sleep(0.05)

    print()
    print(f"RTT     - min: {min(rtts)}ms  max: {max(rtts)}ms  avg: {sum(rtts)//len(rtts)}ms")
    print(f"Offset  - best (min-RTT sample): {best_offset:+}ms")
    print(f"Offset  - avg across all samples: {sum(offsets)//len(offsets):+}ms")
    print()
    direction = "behind" if best_offset > 0 else "ahead of"
    print(f"=> Your local clock is {abs(best_offset)}ms {direction} Binance server time")

except Exception as e:
    import traceback
    print(f"ERROR: {e}")
    traceback.print_exc()
