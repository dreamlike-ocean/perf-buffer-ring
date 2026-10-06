#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_root"
# Only the server and client processes are isolated from each other; their threads are not pinned.
# The server runs on the P cores (0-11) and the client on the E cores (12-19).
server_cpus="${SERVER_CPUS:-0-11}"
client_cpus="${CLIENT_CPUS:-12-19}"
workers="${WORKERS:-2}"
client_loops="${CLIENT_LOOPS:-5}"
ramp_start="${RAMP_START:-20000}"
ramp_step="${RAMP_STEP:-20000}"
ramp_max="${RAMP_MAX:-400000}"
initial_warmup="${INITIAL_WARMUP:-15}"
settle="${SETTLE:-5}"
duration="${DURATION:-15}"
request_bytes="${REQUEST_BYTES:-2048}"
read -r -a blocking_ms_list <<< "${BLOCKING_MS:-5 10 20}"
read -r -a allocators <<< "${ALLOCATORS:-adaptive recycling}"
results_dir="$repo_root/target/perf/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$results_dir"

MAVEN_OPTS="${MAVEN_OPTS:-} --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow" \
    mvn -o -q -pl io-uring-server,perf-client compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
server_classpath="$repo_root/io-uring-server/target/classes:$(cat io-uring-server/target/classpath.txt)"
client_classpath="$repo_root/perf-client/target/classes:$(cat perf-client/target/classpath.txt)"
common_options=(
    -Xms512m -Xmx512m -XX:+UseG1GC
    -Dio.netty.leakDetection.level=disabled -Dio.netty.noUnsafe=false
    --sun-misc-unsafe-memory-access=allow -Dio.netty.tryReflectionSetAccessible=true
    --add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED
)
server_options=("${common_options[@]}" -Dperf.workers="$workers")
client_options=("${common_options[@]}" -Dperf.client.loops="$client_loops")

stop_server() {
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
}


printf 'allocator,blocking_ms,target_qps,h1_qps,h2_qps,total_qps,failed,pool_busy,unfinished,p99_ms,arrival_p99_ms,server_cpu_pct,worker_cpu_pct,client_cpu_pct,client_loop_cpu_pct\n' > "$results_dir/results.csv"
printf 'allocator,blocking_ms,fallback,foreign_thread\n' > "$results_dir/recycling-stats.csv"
printf 'Server CPUs=%s (workers=%s), Client CPUs=%s (loops=%s)\n' "$server_cpus" "$workers" "$client_cpus" "$client_loops"
printf 'ramp=%s..%s step %s QPS, initial warmup=%ss, settle=%ss, measurement=%ss, blocking=%s ms\n' \
    "$ramp_start" "$ramp_max" "$ramp_step" "$initial_warmup" "$settle" "$duration" "${blocking_ms_list[*]}"
for blocking_ms in "${blocking_ms_list[@]}"; do
    for allocator in "${allocators[@]}"; do
        run="$allocator-${blocking_ms}ms"
        taskset -c "$server_cpus" java "${server_options[@]}" -Dperf.blocking.ms="$blocking_ms" -cp "$server_classpath" \
            io.github.dreamlike.proxy.server.HttpProxyServerMain "$allocator" > "$results_dir/$run-server.log" 2>&1 &
        server_pid=$!
        trap stop_server EXIT
        curl --noproxy '*' --silent --fail --retry 10 --retry-connrefused \
            --retry-delay 1 --max-time 2 http://127.0.0.1:4399/ > /dev/null
        printf 'Running %s, blocking %s ms, open-loop ramp\n' "$allocator" "$blocking_ms"
        client_log="$results_dir/$run-ramp-client.log"
        taskset -c "$client_cpus" java "${client_options[@]}" \
            -Dperf.ramp.start="$ramp_start" -Dperf.ramp.step="$ramp_step" -Dperf.ramp.max="$ramp_max" \
            -Dperf.ramp.initialWarmup="$initial_warmup" -Dperf.ramp.settle="$settle" -Dperf.ramp.duration="$duration" \
            -Dperf.server.pid="$server_pid" -Dperf.clk.tck="$(getconf CLK_TCK)" \
            -cp "$client_classpath" io.github.dreamlike.proxy.client.PerfClientMain "$request_bytes" > "$client_log" 2>&1
        # Each step prints the h1/h2/mixed rows and then a cpu row; once all are in, write one result row.
        awk -F, -v allocator="$allocator" -v blocking="$blocking_ms" '
            /^h1,/ { h1 = $3 }
            /^h2,/ { h2 = $3 }
            /^mixed,/ { row = allocator "," blocking "," $2 "," h1 "," h2 "," $3 "," $6 "," $7 "," $8 "," $11 "," $12 }
            /^cpu,/ { printf "%s,%s,%s,%s,%s\n", row, $3, $4, $5, $6 }
        ' "$client_log" >> "$results_dir/results.csv"
        stop_server
        trap - EXIT
        # The server prints its final counters on shutdown; only recycling has this line.
        awk -F'[,=]' -v allocator="$allocator" -v blocking="$blocking_ms" '
            /^recycling_stats,/ { line = allocator "," blocking "," $3 "," $5 }
            END { if (line != "") print line }
        ' "$results_dir/$run-server.log" >> "$results_dir/recycling-stats.csv"
    done
done
# Max QPS: the highest step whose actual QPS reached 95% of the target with no failed, pool-busy or unfinished
# requests, the same criterion RampTest stops on.
awk -F, '
    BEGIN { print "allocator,blocking_ms,target_qps,h1_qps,h2_qps,total_qps,p99_ms,server_cpu_pct,worker_cpu_pct" }
    NR > 1 && $6 >= $3 * 0.95 && $7 == 0 && $8 == 0 && $9 == 0 {
        key = $1 "," $2
        if (!(key in best) || $6 > best[key]) {
            best[key] = $6
            row[key] = $1 "," $2 "," $3 "," $4 "," $5 "," $6 "," $10 "," $12 "," $13
        }
    }
    END { for (key in row) print row[key] }
' "$results_dir/results.csv" > "$results_dir/max-throughput.csv"
cat "$results_dir/results.csv"
cat "$results_dir/max-throughput.csv"
cat "$results_dir/recycling-stats.csv"
printf 'Results: %s\n' "$results_dir"
