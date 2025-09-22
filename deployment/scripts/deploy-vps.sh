#!/bin/bash

# VPS Deployment Script for EasyRent WebApp
# Deploys alongside existing applications

set -e

echo "🚀 Deploying EasyRent WebApp to VPS..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Configuration
VPS_IP=${1:-"localhost"}
DEPLOY_TYPE=${2:-"ports"}  # ports, proxy, or subdomain

print_step "1/5 Checking deployment configuration..."

print_status "VPS IP: $VPS_IP"
print_status "Deployment type: $DEPLOY_TYPE"

# Navigate to project root
cd "$(dirname "$0")/../.."

print_step "2/5 Updating configuration for VPS..."

# Update environment variables
case $DEPLOY_TYPE in
    "ports")
        API_URL="http://$VPS_IP:8082/api"
        FRONTEND_PORT="8081"
        BACKEND_PORT="8082"
        ;;
    "proxy")
        API_URL="http://$VPS_IP/easyrent/api"
        FRONTEND_PORT="8081"
        BACKEND_PORT="8082"
        ;;
    "subdomain")
        API_URL="http://easyrent.$VPS_IP/api"
        FRONTEND_PORT="80"
        BACKEND_PORT="5000"
        ;;
esac

# Create VPS-specific environment file
cat > deployment/config/vps.env << EOF
# VPS Production Environment
FLASK_ENV=production
FLASK_DEBUG=false
FLASK_HOST=0.0.0.0
FLASK_PORT=5000

VITE_API_URL=$API_URL
VITE_APP_TITLE=EasyRent Management System
VITE_APP_ENVIRONMENT=production

MAX_CONTENT_LENGTH=52428800
UPLOAD_TIMEOUT=300000

CORS_ORIGINS=["http://$VPS_IP:$FRONTEND_PORT", "http://$VPS_IP", "https://$VPS_IP"]

LOG_LEVEL=INFO
SECRET_KEY=vps-production-secret-key-change-me
JWT_SECRET_KEY=vps-jwt-secret-key-change-me
EOF

print_step "3/5 Building Docker containers..."

# Choose the right docker-compose file
if [ "$DEPLOY_TYPE" = "ports" ]; then
    COMPOSE_FILE="deployment/docker/docker-compose-vps.yml"
else
    COMPOSE_FILE="deployment/docker/docker-compose.yml"
fi

# Stop any existing containers
print_status "Stopping existing EasyRent containers..."
docker-compose -f $COMPOSE_FILE down --remove-orphans 2>/dev/null || true

# Build and start containers
print_status "Building containers..."
docker-compose -f $COMPOSE_FILE build --no-cache

print_status "Starting containers..."
docker-compose -f $COMPOSE_FILE up -d

print_step "4/5 Configuring reverse proxy (if needed)..."

if [ "$DEPLOY_TYPE" = "proxy" ]; then
    print_warning "Manual nginx configuration required!"
    print_status "Copy deployment/nginx/vps-nginx.conf to your nginx sites-available"
    print_status "Update the configuration with your existing app details"
    print_status "Enable the site and reload nginx"
fi

print_step "5/5 Verifying deployment..."

# Wait for services to start
sleep 10

# Check if containers are running
if docker-compose -f $COMPOSE_FILE ps | grep -q "Up"; then
    print_status "✅ EasyRent WebApp deployed successfully!"
    echo ""
    echo "🌐 Access URLs:"
    case $DEPLOY_TYPE in
        "ports")
            echo "   Frontend: http://$VPS_IP:$FRONTEND_PORT"
            echo "   Backend API: http://$VPS_IP:$BACKEND_PORT/api"
            ;;
        "proxy")
            echo "   Frontend: http://$VPS_IP/easyrent"
            echo "   Backend API: http://$VPS_IP/easyrent/api"
            echo "   ⚠️  Configure nginx reverse proxy manually"
            ;;
        "subdomain")
            echo "   Frontend: http://easyrent.$VPS_IP"
            echo "   Backend API: http://easyrent.$VPS_IP/api"
            echo "   ⚠️  Configure DNS and nginx for subdomain"
            ;;
    esac
    echo ""
    echo "📊 Container Status:"
    docker-compose -f $COMPOSE_FILE ps
    echo ""
    echo "🔧 Management Commands:"
    echo "   View logs: docker-compose -f $COMPOSE_FILE logs -f"
    echo "   Stop: docker-compose -f $COMPOSE_FILE down"
    echo "   Restart: docker-compose -f $COMPOSE_FILE restart"
    echo ""
    echo "⚠️  Important Notes:"
    echo "   1. Update firewall rules to allow ports $FRONTEND_PORT and $BACKEND_PORT"
    echo "   2. Change default secret keys in deployment/config/vps.env"
    echo "   3. Set up SSL certificates for production use"

    if [ "$DEPLOY_TYPE" = "proxy" ]; then
        echo "   4. Configure nginx reverse proxy using deployment/nginx/vps-nginx.conf"
    fi
else
    print_error "❌ Deployment failed. Check logs:"
    docker-compose -f $COMPOSE_FILE logs
    exit 1
fi