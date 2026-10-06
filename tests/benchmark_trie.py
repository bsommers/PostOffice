import time
from postoffice.trie import TopicTrie

def linear_topic_match(route_topic: str, message_topic: str) -> bool:
    """Original linear matching implementation for benchmark comparison."""
    if route_topic == message_topic:
        return True

    route_parts = route_topic.split('/')
    msg_parts = message_topic.split('/')

    for i, part in enumerate(route_parts):
        if part == '#':
            return True
        if i >= len(msg_parts):
            return False
        if part != '+' and part != msg_parts[i]:
            return False

    return len(route_parts) == len(msg_parts)

def linear_scan_match(routes: list[tuple[str, dict]], message_topic: str) -> list[dict]:
    results = []
    for r_topic, config in routes:
        if linear_topic_match(r_topic, message_topic):
            results.append(config)
    return results

def run_benchmark():
    route_scales = [10, 100, 1000, 10000]
    iterations = 5000

    print("=" * 78)
    print(f"PostOffice Route Engine Benchmark: Linear Scan vs. Hierarchical TopicTrie")
    print(f"Iterations per scale: {iterations:,} message lookups")
    print("=" * 78)
    print(f"{'Routes (N)':<12} | {'Linear Time':<12} | {'Trie Time':<12} | {'Trie Speedup':<14} | {'Trie Throughput'}")
    print("-" * 78)

    test_queries = [
        "sensors/facility/alpha/temp",
        "sensors/facility/beta/humidity",
        "devices/gateway/status",
        "telemetry/production/line_42/voltage",
        "alerts/critical/system/failure"
    ]

    for n in route_scales:
        # Generate synthetic routes with realistic mixture of exact, '+', and '#'
        routes_list = []
        trie = TopicTrie()

        for i in range(n):
            if i % 10 == 0:
                pattern = f"sensors/facility/+/temp"
            elif i % 7 == 0:
                pattern = f"telemetry/production/#"
            elif i % 5 == 0:
                pattern = f"devices/+/{i}"
            else:
                pattern = f"sensors/facility/site_{i % 50}/temp"

            config = {"target": f"dest_{i}", "topic": f"out_{i}"}
            routes_list.append((pattern, config))
            trie.insert(pattern, config)

        # Warmup
        for q in test_queries:
            linear_scan_match(routes_list, q)
            trie.match(q)

        # Benchmark Linear Scan
        t0 = time.perf_counter()
        for i in range(iterations):
            q = test_queries[i % len(test_queries)]
            linear_scan_match(routes_list, q)
        t_linear = time.perf_counter() - t0

        # Benchmark TopicTrie
        t0 = time.perf_counter()
        for i in range(iterations):
            q = test_queries[i % len(test_queries)]
            trie.match(q)
        t_trie = time.perf_counter() - t0

        speedup = t_linear / t_trie if t_trie > 0 else float('inf')
        trie_ops = iterations / t_trie if t_trie > 0 else 0

        print(f"{n:<12} | {t_linear*1000:>9.2f} ms | {t_trie*1000:>9.2f} ms | {speedup:>12.1f}x | {trie_ops:>10,.0f} ops/sec")

    print("=" * 78)

if __name__ == '__main__':
    run_benchmark()
