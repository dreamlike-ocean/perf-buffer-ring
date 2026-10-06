#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_root"
server_cpu="${SERVER_CPU:-2}"
client_cpu="${CLIENT_CPU:-4}"
duration="${DURATION:-30}"
warmup="${WARMUP:-30}"
request_bytes="${REQUEST_BYTES:-2048}"
read -r -a loads <<< "${LOADS:-1000 2000 4000 max}"
results_dir="$repo_root/target/perf/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$results_dir"

MAVEN_OPTS="${MAVEN_OPTS:-} --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
    mvn -o -q -pl io-uring-server,perf-client compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
server_classpath="$repo_root/io-uring-server/target/classes:$(cat io-uring-server/target/classpath.txt)"
client_classpath="$repo_root/perf-client/target/classes:$(cat perf-client/target/classpath.txt)"
java_options=(
    -Xms512m -Xmx512m -XX:+UseG1GC -XX:ActiveProcessorCount=1
    -Dio.netty.leakDetection.level=disabled -Dio.netty.noUnsafe=false
    --sun-misc-unsafe-memory-access=allow -Dio.netty.tryReflectionSetAccessible=true
    --add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED
)

stop_server() {
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
}

printf 'allocator,target_qps,h1_qps,h2_qps,total_qps,failed,pool_busy,unfinished,p99_ms,arrival_p99_ms\n' > "$results_dir/results.csv"
printf 'Server CPU=%s, Client CPU=%s, warmup=%ss, measurement=%ss\n' "$server_cpu" "$client_cpu" "$warmup" "$duration"
for allocator in adaptive recycling; do
    taskset -c "$server_cpu" java "${java_options[@]}" -cp "$server_classpath" \
        io.github.dreamlike.proxy.server.HttpProxyServerMain "$allocator" > "$results_dir/$allocator-server.log" 2>&1 &
    server_pid=$!
    trap stop_server EXIT
    curl --noproxy '*' --silent --fail --retry 10 --retry-connrefused \
        --retry-delay 1 --max-time 2 http://127.0.0.1:4399/ > /dev/null
    for load in "${loads[@]}"; do
        printf 'Running %s at %s QPS\n' "$allocator" "$load"
        client_log="$results_dir/$allocator-$load-client.log"
        taskset -c "$client_cpu" java "${java_options[@]}" -cp "$client_classpath" \
            io.github.dreamlike.proxy.client.PerfClientMain "$load" "$duration" "$warmup" "$request_bytes" > "$client_log" 2>&1
        awk -F, -v allocator="$allocator" '
            /^h1,/ { h1 = $3 }
            /^h2,/ { h2 = $3 }
            /^mixed,/ { printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", allocator, $2, h1, h2, $3, $6, $7, $8, $11, $12 }
        ' "$client_log" >> "$results_dir/results.csv"
    done
    stop_server
    trap - EXIT
done
awk -F, '
    BEGIN { print "allocator,concurrent_pairs,h1_qps,h2_qps,total_qps,p99_ms" }
    $2 ~ /^max-/ && $6 == 0 && $7 == 0 && $8 == 0 && $5 > best[$1] {
        best[$1] = $5
        pairs = $2
        sub(/^max-/, "", pairs)
        row[$1] = $1 "," pairs "," $3 "," $4 "," $5 "," $9
    }
    END { for (allocator in row) print row[allocator] }
' "$results_dir/results.csv" > "$results_dir/max-throughput.csv"
cat "$results_dir/results.csv"
cat "$results_dir/max-throughput.csv"
printf 'Results: %s\n' "$results_dir"
