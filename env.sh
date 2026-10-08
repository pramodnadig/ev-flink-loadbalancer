#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# env.sh - one-line environment setup for the EV Charging Load Balancer project
#
# Usage (from anywhere, in every new Terminal window):
#     source ~/ev-flink/ev-flink-loadbalancer/env.sh
#
# What it does:
#   1. Points this Terminal at Java 11 (Flink 1.20 does not support Java 23).
#      Only this window is affected - your system Java stays as it is.
#   2. Puts the Flink 1.20.5 commands (flink, start-cluster.sh, ...) on the PATH.
#   3. Activates the project's Python 3.11 virtual environment (PyFlink lives there).
#   4. Prints a quick health check so you can see everything is wired up.
# -----------------------------------------------------------------------------

# Folder that contains this script = the project root
export PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

# Parent folder (~/ev-flink) holds the Flink download and the virtual environment
export EV_BASE="$(dirname "$PROJECT_HOME")"

export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
export FLINK_HOME="$EV_BASE/flink-1.20.5"

# PyFlink should use the same Python as the virtual environment.
# Activate it FIRST: re-activating a venv restores the PATH it saw originally,
# which would otherwise remove the Flink/Java entries added below.
if [ -f "$EV_BASE/.venv/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "$EV_BASE/.venv/bin/activate"
    export PYFLINK_CLIENT_EXECUTABLE="$EV_BASE/.venv/bin/python"
    export PYFLINK_EXECUTABLE="$EV_BASE/.venv/bin/python"
fi
export PATH="$JAVA_HOME/bin:$FLINK_HOME/bin:$PATH"

# Make project modules importable (generator, flink_job, analysis, ...)
export PYTHONPATH="$PROJECT_HOME:$PYTHONPATH"

# Kafka (installed with Homebrew) and the Flink-Kafka connector jar
export KAFKA_BOOTSTRAP="localhost:9092"
export KAFKA_CONNECTOR_JAR="$EV_BASE/lib/flink-sql-connector-kafka-3.4.0-1.20.jar"

echo "----------------------------------------------------------"
echo " EV Charging Load Balancer - environment ready"
echo "----------------------------------------------------------"
echo " Project : $PROJECT_HOME"
echo " Java    : $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
echo " Python  : $(python --version 2>&1)"
if [ -x "$FLINK_HOME/bin/flink" ]; then
    echo " Flink   : $FLINK_HOME"
else
    echo " Flink   : NOT FOUND at $FLINK_HOME  (check the folder name)"
fi
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/overview 2>/dev/null | grep -q 200; then
    echo " Cluster : running  -> http://localhost:8081"
else
    echo " Cluster : not running (start with: start-cluster.sh)"
fi
if nc -z localhost 9092 2>/dev/null; then
    echo " Kafka   : running  -> $KAFKA_BOOTSTRAP"
else
    echo " Kafka   : not running (start with: brew services start kafka)"
fi
if [ -f "$KAFKA_CONNECTOR_JAR" ]; then
    echo " Kafka connector jar : OK"
else
    echo " Kafka connector jar : missing (run: bash scripts/setup_kafka.sh)"
fi
echo "----------------------------------------------------------"
