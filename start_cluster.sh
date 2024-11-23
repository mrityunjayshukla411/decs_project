#!/bin/bash
# start_cluster.sh

# Function to handle script termination
cleanup() {
    echo "Stopping all server nodes..."
    pkill -f "python3 server.py"  # Kill all nodes by process name
    exit 0
}

# Trap termination signals (SIGINT, SIGTERM) to call the cleanup function
trap cleanup SIGINT SIGTERM

# Start first node
taskset -c 0 python3 server.py --port 5000 &
PID1=$!
echo "Started node on port 5000 (PID $PID1)"
sleep 2

# Start second node
taskset -c 1 python3 server.py --port 5001 --cluster localhost:5000 &
PID2=$!
echo "Started node on port 5001 (PID $PID2)"
sleep 2

# Start third node
taskset -c 2 python3 server.py --port 5002 --cluster localhost:5000 localhost:5001 &
PID3=$!
echo "Started node on port 5002 (PID $PID3)"
sleep 2

echo "Cluster started. Press Ctrl+C to stop all nodes."

# Wait for all background processes
wait $PID1 $PID2 $PID3