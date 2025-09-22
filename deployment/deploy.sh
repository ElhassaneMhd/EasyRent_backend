#!/bin/bash

# EasyRent Backend Deployment Script
# Run this script on your VPS to deploy the application

set -e  # Exit on error

echo "========================================="
echo "EasyRent Backend Deployment Script"
echo "========================================="

# Variables
APP_DIR="/var/www/easyrent"
REPO_URL="https://github.com/ElhassaneMhd/EasyRent_backend.git"  # Update with your repo URL
BRANCH="main"
SERVICE_NAME="easyrent"
NGINX_SITE="easyrent"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   print_error "This script must be run as root"
   exit 1
fi

# Step 1: Install required packages
print_status "Installing required system packages..."
apt-get update
apt-get install -y python3 python3-pip python3-venv nginx git supervisor

# Step 2: Create application directory
print_status "Creating application directory..."
mkdir -p $APP_DIR
mkdir -p $APP_DIR/uploads
mkdir -p $APP_DIR/outputs
mkdir -p /var/log/easyrent

# Step 3: Clone or update repository
if [ -d "$APP_DIR/.git" ]; then
    print_status "Updating existing repository..."
    cd $APP_DIR
    git pull origin $BRANCH
else
    print_status "Cloning repository..."
    git clone $REPO_URL $APP_DIR
    cd $APP_DIR
    git checkout $BRANCH
fi

# Step 4: Set up Python virtual environment
print_status "Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Step 5: Install Python dependencies
print_status "Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Step 6: Copy environment configuration
if [ ! -f "$APP_DIR/.env" ]; then
    print_status "Creating environment configuration..."
    cp .env.production .env
    print_warning "Please edit $APP_DIR/.env and update the configuration values"
fi

# Step 7: Set permissions
print_status "Setting permissions..."
chown -R www-data:www-data $APP_DIR
chmod -R 755 $APP_DIR
chmod -R 775 $APP_DIR/uploads
chmod -R 775 $APP_DIR/outputs

# Step 8: Configure systemd service
print_status "Configuring systemd service..."
cp deployment/easyrent.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable $SERVICE_NAME

# Step 9: Configure Nginx
print_status "Configuring Nginx..."
cp deployment/nginx-easyrent.conf /etc/nginx/sites-available/$NGINX_SITE
ln -sf /etc/nginx/sites-available/$NGINX_SITE /etc/nginx/sites-enabled/

# Step 10: Test Nginx configuration
print_status "Testing Nginx configuration..."
nginx -t

# Step 11: Restart services
print_status "Restarting services..."
systemctl restart $SERVICE_NAME
systemctl restart nginx

# Step 12: Check service status
print_status "Checking service status..."
if systemctl is-active --quiet $SERVICE_NAME; then
    print_status "EasyRent service is running"
else
    print_error "EasyRent service failed to start"
    print_warning "Check logs: journalctl -u $SERVICE_NAME -n 50"
fi

# Step 13: Setup SSL certificate (optional)
print_warning "To setup SSL certificate with Let's Encrypt, run:"
echo "certbot --nginx -d easyrent.zmachine.pro"

print_status "Deployment complete!"
echo ""
echo "Next steps:"
echo "1. Edit $APP_DIR/.env and update configuration"
echo "2. Setup SSL certificate with: certbot --nginx -d easyrent.zmachine.pro"
echo "3. Restart service: systemctl restart $SERVICE_NAME"
echo "4. Check logs: tail -f /var/log/easyrent/error.log"
echo ""
echo "Your API should be available at: https://easyrent.zmachine.pro/api"