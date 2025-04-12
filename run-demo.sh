#!/bin/bash

# Compile the project
echo "Compiling project..."
mvn clean package

# Run the master (in background)
echo "Starting master..."
java -cp target/lru-object-store-vertx-1.0-SNAPSHOT.jar com.example.MainApp &
MASTER_PID=$!
sleep 5

# Function to generate large values
generate_large_json() {
  SIZE_MB=$1
  python3 -c "import json; print(json.dumps({'data': 'x' * (1024 * 1024 * $SIZE_MB)}))"
}

# Insert large objects to fill memory
for i in {1..15}
do
  VALUE=$(generate_large_json 150)  # ~150MB per object
  echo "Putting object key=item$i"
  java -cp target/lru-object-store-vertx-1.0-SNAPSHOT.jar \
    io.vertx.core.Launcher run com.example.cli.CacheClientVerticle \
    -Dcmd=put -Dkey=item$i -Dvalue="$VALUE"
  sleep 1
done

# Wait a bit to let all processes finish
sleep 10

# Show running Java processes (should include spawned slaves)
echo "Java processes:"
jps

# Cleanup
echo "Stopping master..."
kill $MASTER_PID