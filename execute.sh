#!/bin/bash

# add options to start and stop as cli params
usage() {
    echo "Usage: $0 [start|stop|test]"
    exit 1
}

validate_docker() {
  # check if docker services are running
  if ! docker ps > /dev/null 2>&1; then
      echo "Docker is not running. Please start Docker and try again."
      exit 1
  fi

  # check if the docker-compose.yml services are running
  if ! docker-compose ps > /dev/null 2>&1; then
      echo "Docker Compose services are not running. Please start them with 'docker-compose up' and try again."
  else
      echo "Docker Compose services are running."
      docker compose down -v
  fi
}

build_flink_program() {
  # clean target directory
  if [ -d "target" ]; then
      echo "Cleaning target directory..."
      rm -rf target
  else
      echo "Target directory does not exist, skipping clean."
  fi

  mvn clean package -DskipTests
  if [ $? -ne 0 ]; then
      echo "Maven build failed. Please check the output for errors."
      exit 1
  fi
}

start_kafka_cluster() {
  # start services apart from flink
  docker compose up -d zookeeper kafka kafka-ui

  # wait for kafka to be ready
  echo "Waiting for Kafka to be ready..."
  while ! docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    echo "Kafka is not ready yet. Waiting..."
    sleep 5
  done
  echo "Kafka is ready."
}

create_topics() {
  echo "Creating Kafka topics..."

  topics=(
    "user-events"
    "address-events"
    "enriched-users"
    "user-changes"
  )

  for topic in "${topics[@]}"; do
    if ! docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep -q "$topic"; then
      echo "Creating topic: $topic"
      docker exec -it kafka kafka-topics --create --topic "$topic" --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
    else
      echo "Topic $topic already exists."
    fi
  done
}

submit_job() {
    echo "Submitting Flink job..."
    docker compose up -d flink-submit
}

start_job_and_task_manager() {
    echo "Starting Flink JobManager and TaskManager..."
    docker compose up -d flink-jobmanager flink-taskmanager
    # wait for flink job manager to be read
    echo "Waiting for Flink JobManager to be ready..."
    while ! docker exec flink-jobmanager curl -s http://localhost:8081 > /dev/null; do
        echo "Flink JobManager is not ready yet. Waiting..."
        sleep 5
    done
    echo "Flink JobManager is ready."
}

stop_services() {
    docker compose down -v
    echo "All services stopped."
}

start_services() {
    validate_docker

    # build the project
    build_flink_program

    start_kafka_cluster

    # prepare topics in kafka if not exists
    create_topics

    # start flink job and task manager
    start_job_and_task_manager

    # run the Flink job
    submit_job
}

test() {
  # Create Python virtual environment if not exists
  VENV_DIR="venv"
  if [ ! -d "$VENV_DIR" ]; then
      python -m venv "$VENV_DIR"
  fi

  # Activate the virtual environment
  source "$VENV_DIR/bin/activate"

  # Install dependencies from requirements.txt
  pip install -r requirements.txt

  # Run the Python script
  python scripts/send_events.py
}

if [ $# -ne 1 ]; then
    usage
fi

case $1 in
    start)
        echo "Starting the services..."
        start_services
        ;;
    stop)
        echo "Stopping the services..."
        stop_services
        exit 0
        ;;
    test)
        echo "Running tests..."
        test
        ;;
    *)
        usage
        ;;
esac