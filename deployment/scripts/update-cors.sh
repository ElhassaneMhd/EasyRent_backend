#!/bin/bash

# CORS Update Script for VPS Backend after Netlify deployment

set -e

echo "🔧 Updating CORS configuration for Netlify deployment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Get Netlify URL from user
NETLIFY_URL=${1:-""}

if [ -z "$NETLIFY_URL" ]; then
    echo "Please enter your Netlify URL (e.g., https://amazing-app-123456.netlify.app):"
    read -r NETLIFY_URL
fi

# Validate URL format
if [[ ! "$NETLIFY_URL" =~ ^https?:// ]]; then
    NETLIFY_URL="https://$NETLIFY_URL"
fi

# Remove trailing slash
NETLIFY_URL=$(echo "$NETLIFY_URL" | sed 's/\/$//')

print_status "Netlify URL: $NETLIFY_URL"

# Navigate to project root
cd "$(dirname "$0")/../.."

print_step "1/3 Updating backend CORS configuration..."

# Backup existing config
if [ -f "deployment/config/vps-backend.env" ]; then
    cp deployment/config/vps-backend.env deployment/config/vps-backend.env.backup
    print_status "Backed up existing config to vps-backend.env.backup"
fi

# Update CORS configuration
cat > deployment/config/vps-backend.env << EOF
# VPS Backend Configuration - Updated for Netlify
FLASK_ENV=production
FLASK_DEBUG=false
FLASK_HOST=0.0.0.0
FLASK_PORT=5000

# CORS - Updated with Netlify URL
CORS_ORIGINS=["$NETLIFY_URL", "https://*.netlify.app", "http://localhost:3000", "http://localhost:5173"]

# File upload settings
MAX_CONTENT_LENGTH=52428800
UPLOAD_TIMEOUT=300000

# Security
SECRET_KEY=change-this-secret-key-in-production
JWT_SECRET_KEY=change-this-jwt-secret-key-in-production

# Logging
LOG_LEVEL=INFO
LOG_FILE=easyrent.log
EOF

print_step "2/3 Updating Docker environment..."

# Update docker-compose environment
if [ -f "deployment/docker/docker-compose-backend.yml" ]; then
    # Add environment variables to docker-compose
    sed -i.bak '/environment:/,/volumes:/ {
        /environment:/a\
      - CORS_ORIGINS=["'$NETLIFY_URL'", "https://*.netlify.app"]
    }' deployment/docker/docker-compose-backend.yml

    print_status "Updated docker-compose-backend.yml"
fi

print_step "3/3 Creating restart script..."

# Create script to restart backend with new CORS
cat > restart-backend.sh << 'EOF'
#!/bin/bash

echo "🔄 Restarting backend with new CORS configuration..."

# Navigate to docker directory
cd deployment/docker

# Restart the backend container
docker-compose -f docker-compose-backend.yml restart easyrent-backend

# Wait for container to start
sleep 5

# Check if container is running
if docker-compose -f docker-compose-backend.yml ps | grep -q "Up"; then
    echo "✅ Backend restarted successfully!"
    echo ""
    echo "🧪 Testing CORS configuration..."

    # Test CORS with the new Netlify URL
    echo "Testing API health endpoint..."
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:5000/api/health | grep -q "200"; then
        echo "✅ API health check passed"
    else
        echo "❌ API health check failed"
    fi

else
    echo "❌ Failed to restart backend. Check logs:"
    docker-compose -f docker-compose-backend.yml logs easyrent-backend
fi
EOF

chmod +x restart-backend.sh

print_status "✅ CORS configuration updated!"

echo ""
echo "📋 Summary:"
echo "   Updated CORS origins: $NETLIFY_URL"
echo "   Backup created: deployment/config/vps-backend.env.backup"
echo "   Restart script: ./restart-backend.sh"
echo ""
echo "🚀 Next steps:"
echo "   1. Copy the updated config to your VPS:"
echo "      scp deployment/config/vps-backend.env user@your-vps:/path/to/easyrent/"
echo ""
echo "   2. SSH to your VPS and restart the backend:"
echo "      ssh user@your-vps"
echo "      cd /path/to/easyrent"
echo "      ./restart-backend.sh"
echo ""
echo "   3. Test your application:"
echo "      Frontend: $NETLIFY_URL"
echo "      Backend: Your VPS API endpoint"

print_warning "Don't forget to update the secret keys in production!"

# Test if we're on the VPS (check if docker is available)
if command -v docker &> /dev/null; then
    echo ""
    echo "🤔 Detected Docker available. Are you running this on your VPS?"
    echo "Would you like to restart the backend now? (y/n)"
    read -r RESTART_NOW

    if [[ "$RESTART_NOW" =~ ^[Yy]$ ]]; then
        print_status "Restarting backend..."
        ./restart-backend.sh
    fi
fi