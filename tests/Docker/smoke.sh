set -eu

image=murmur-smoke
secret=smoke-secret-with-at-least-32-bytes
container=
failing=

cleanup() {
    for id in $container $failing; do
        docker rm -f "$id" > /dev/null 2>&1 || true
    done
}

fail() {
    echo "smoke: $1" >&2
    for id in $container $failing; do
        docker logs "$id" >&2 || true
    done
    exit 1
}

recorded_at() {
    docker exec "$container" php vendor/bin/testbench murmur:status --json 2> /dev/null \
        | sed -n 's/.*"recorded_at":\([0-9]*\).*/\1/p' | head -n 1
}

writer_pid() {
    docker exec "$container" pgrep -f 'murmur:record-metrics' | tr '\n' ' '
}

trap cleanup EXIT

docker build --quiet --file tests/Docker/Dockerfile --tag "$image" . > /dev/null

container=$(docker run --detach \
    --env MURMUR_SECRET="$secret" \
    --env MURMUR_METRICS_STORE=cache \
    --env MURMUR_METRICS_CACHE_STORE=file \
    --env MURMUR_METRICS_INTERVAL=1 \
    --env MURMUR_METRICS_TTL=30 \
    "$image")

docker exec "$container" sh -c "tr '\\0' ' ' < /proc/1/cmdline" | grep -q 'murmur:serve' \
    || fail 'the server is not the init process of the container'
docker exec "$container" sh -c 'command -v bash' > /dev/null 2>&1 \
    && fail 'the image has bash, so it does not prove the recorder works without it'

first=
attempts=0
while [ -z "$first" ]; do
    attempts=$((attempts + 1))
    [ "$attempts" -le 30 ] || fail 'no snapshot was stored'
    sleep 1
    first=$(recorded_at)
done
pid=$(writer_pid)

later=$first
attempts=0
while [ "$later" -lt $((first + 3)) ]; do
    attempts=$((attempts + 1))
    [ "$attempts" -le 30 ] || fail 'snapshots stopped being stored'
    sleep 1
    later=$(recorded_at)
done

[ "$(writer_pid)" = "$pid" ] || fail 'the writer was replaced'
[ "$(echo "$pid" | wc -w)" -eq 1 ] || fail 'expected exactly one writer'

docker stop --time 10 "$container" > /dev/null
[ "$(docker inspect --format '{{.State.ExitCode}}' "$container")" -eq 0 ] || fail 'the server did not exit cleanly'
docker logs "$container" 2>&1 | grep -q 'recording has stopped' && fail 'the writer exited while serving'

failing=$(docker run --detach \
    --env MURMUR_SECRET="$secret" \
    --env MURMUR_METRICS_STORE=database \
    --env MURMUR_METRICS_INTERVAL=1 \
    --env DB_CONNECTION=sqlite \
    --env DB_DATABASE=:memory: \
    --env LOG_CHANNEL=stderr \
    "$image")

attempts=0
until docker logs "$failing" 2>&1 | grep -q 'murmur_metrics'; do
    attempts=$((attempts + 1))
    [ "$attempts" -le 30 ] || fail 'the cause of a failed write did not reach the container log'
    sleep 1
done

docker stop --time 10 "$failing" > /dev/null
[ "$(docker inspect --format '{{.State.ExitCode}}' "$failing")" -eq 0 ] || fail 'the server with a failing store did not exit cleanly'

echo 'smoke: ok'
