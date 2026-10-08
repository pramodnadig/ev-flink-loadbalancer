#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# setup_kafka.sh - one-time Kafka setup for the EV Charging Load Balancer
#
# Prerequisite (once):   brew install kafka
# Usage:                 source ~/ev-flink/ev-flink-loadbalancer/env.sh
#                        bash scripts/setup_kafka.sh
#
# What it does (safe to run again; finished steps are skipped):
#   1. Formats Kafka's storage folder (KRaft mode, no ZooKeeper) if not done yet
#   2. Starts Kafka as a background service on localhost:9092
#   3. Downloads the Flink-Kafka connector (lets Flink read from Kafka)
#   4. Installs the Python Kafka client into the project environment
#   5. Runs a smoke test: create a topic, send 3 messages, read them back
#
# Note: Kafka 4.x needs Java 17+. Homebrew installed its own Java for Kafka, so
# the Kafka commands below run with JAVA_HOME unset (Flink keeps using Java 11).
# -----------------------------------------------------------------------------
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EV_BASE="$(dirname "$PROJECT_HOME")"
JAR_VERSION="3.4.0-1.20"
JAR_NAME="flink-sql-connector-kafka-${JAR_VERSION}.jar"
JAR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${JAR_VERSION}/${JAR_NAME}"
LIB_DIR="$EV_BASE/lib"

step() { echo; echo "== $1"; }

step "1/5 Checking Kafka installation"
if ! brew list kafka >/dev/null 2>&1; then
  echo "Kafka is not installed. Run:  brew install kafka   then run this script again."
  exit 1
fi
CONF="$(brew --prefix)/etc/kafka/server.properties"
echo "Kafka config: $CONF"
LOGDIR="$(grep -E '^log.dirs=' "$CONF" | head -1 | cut -d= -f2 | cut -d, -f1)"
echo "Data folder : $LOGDIR"

if [ -f "$LOGDIR/meta.properties" ]; then
  echo "Storage already formatted - skipping."
else
  echo "Formatting storage (first run only)..."
  CLUSTER_ID="$(env -u JAVA_HOME kafka-storage random-uuid)"
  env -u JAVA_HOME kafka-storage format --standalone -t "$CLUSTER_ID" -c "$CONF" \
    || env -u JAVA_HOME kafka-storage format -t "$CLUSTER_ID" -c "$CONF"
fi

step "2/5 Starting Kafka service"
env -u JAVA_HOME brew services restart kafka >/dev/null
for i in $(seq 1 40); do
  if nc -z localhost 9092 2>/dev/null; then echo "Kafka is up on localhost:9092"; break; fi
  sleep 1
  if [ "$i" = 40 ]; then
    echo "Kafka did not start within 40 s. Check the log with:"
    echo "  tail -50 \$(brew --prefix)/var/log/kafka/kafka_output.log"
    exit 1
  fi
done

step "3/5 Flink Kafka connector"
mkdir -p "$LIB_DIR"
if [ -s "$LIB_DIR/$JAR_NAME" ]; then
  echo "Already downloaded: $LIB_DIR/$JAR_NAME"
else
  curl -fL --retry 3 -o "$LIB_DIR/$JAR_NAME" "$JAR_URL"
fi

step "4/5 Python Kafka client"
python -m pip install --quiet "kafka-python>=2.2,<3"
python -c "import kafka; print('kafka-python', kafka.__version__)"

step "5/5 Smoke test"
python - <<'EOF'
import time
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

topic = "ev-smoke-test"
admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
try:
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
except TopicAlreadyExistsError:
    pass
p = KafkaProducer(bootstrap_servers="localhost:9092")
for i in range(3):
    p.send(topic, f"hello-{i}".encode())
p.flush()
c = KafkaConsumer(topic, bootstrap_servers="localhost:9092", auto_offset_reset="earliest",
                  consumer_timeout_ms=5000)
got = [m.value.decode() for m in c]
print("received:", got[-3:])
assert len(got) >= 3, "smoke test failed"
admin.delete_topics([topic])
print("Kafka smoke test OK")
EOF

echo
echo "Kafka setup complete."
