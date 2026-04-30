#!/bin/bash
set -e

echo "BK Video Worker container started."
echo "Polling Supabase every 90 seconds..."

while true; do
    python worker.py
    echo "Sleeping 90s..."
    sleep 90
done
