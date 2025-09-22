#!/bin/bash

# Check if port 5000 is in use and what's using it

echo "========================================="
echo "Port 5000 Usage Check"
echo "========================================="

# Check if port 5000 is listening
echo "1. Checking if port 5000 is in use:"
if netstat -tulpn | grep :5000 > /dev/null; then
    echo "❌ Port 5000 is already in use:"
    netstat -tulpn | grep :5000
    echo ""

    # Get process details
    echo "2. Process details using port 5000:"
    lsof -i :5000
    echo ""

    # Show running services that might be using port 5000
    echo "3. Checking systemd services:"
    systemctl list-units --type=service --state=active | grep -E "(flask|python|gunicorn|uwsgi)" || echo "No obvious Python/Flask services found"
    echo ""

    echo "4. To stop the process using port 5000:"
    PID=$(lsof -t -i:5000)
    if [ ! -z "$PID" ]; then
        echo "Process ID: $PID"
        echo "Command to kill: sudo kill -9 $PID"
        echo "Or if it's a systemd service, use: sudo systemctl stop <service-name>"
    fi

else
    echo "✅ Port 5000 is available"
fi

echo ""
echo "5. Alternative: Change EasyRent to use a different port"
echo "   Edit .env file and change PORT=5001 (or any available port)"
echo "   Then update nginx config to proxy to the new port"