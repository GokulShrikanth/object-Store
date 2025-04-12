#!/bin/bash

# Compile the project
echo "Compiling project..."
mvn clean package

# Run the master (in background)
echo "Starting master..."
java -cp target/object-store-1.0-SNAPSHOT.jar com.objstore.MainApp &
MASTER_PID=$!

# Give master time to fully start
echo "Waiting for master to initialize (15 seconds)..."
sleep 15  # Increased to ensure full initialization

# Function to generate large values with proper escaping
generate_large_json() {
  SIZE_MB=$1
  # Use a temporary file to avoid command line limitations with large strings
  python3 -c "import json; print(json.dumps({'data': 'x' * (1024 * 1024 * $SIZE_MB)}))" > /tmp/large_value.json
  cat /tmp/large_value.json
}

# Try a small test request first to validate connectivity
echo "Sending test PUT request..."
java -cp target/object-store-1.0-SNAPSHOT.jar \
  io.vertx.core.Launcher run com.objstore.cli.CacheClientVerticle \
  -Dcmd=put -Dkey=test -Dvalue="test-value"

# Wait to see if the test was successful
sleep 2

# Insert large objects to fill memory
for i in {1..5}
do
  echo "Putting object key=item$i"
  # Generate value and pass via file to avoid command line length limitations
  generate_large_json 50 > /tmp/large_value_$i.json

  echo "Sending PUT request for item$i..."
  java -cp target/object-store-1.0-SNAPSHOT.jar \
    io.vertx.core.Launcher run com.objstore.cli.CacheClientVerticle \
    -Dcmd=put -Dkey=item$i -Dvalue="@/tmp/large_value_$i.json"

  sleep 2
done

# Show running Java processes (should include spawned slaves)
echo "Java processes:"
jps

# Wait to observe behavior
sleep 10

# Cleanup
echo "Stopping master..."
kill $MASTER_PID

# Clean up temporary files
rm -f /tmp/large_value*.json
