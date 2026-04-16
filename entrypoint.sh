#!/bin/bash
set -e

echo "BK Video Worker container started."
echo "Polling Supabase every 5 minutes..."

while true; do
    python worker.py
    echo "Sleeping 5 min..."
    sleep 300
done
