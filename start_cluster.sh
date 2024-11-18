#!/bin/bash
# start_cluster.sh

# Start first node
python server.py --port 5000 &
echo "Started node on port 5000"
sleep 2

# Start second node
python server.py --port 5001 --cluster localhost:5000 &
echo "Started node on port 5001"
sleep 2

# Start third node
python server.py --port 5002 --cluster localhost:5000 localhost:5001 &
echo "Started node on port 5002"
sleep 2

echo "Cluster started. Press Ctrl+C to stop all nodes"
wait