#!/bin/bash
set -e

echo "======================================================"
echo "  Deploying GymOpus ADMS Biometric Server on Port 80  "
echo "======================================================"

# 1. Install Node.js 20 LTS if not installed
if ! command -v node &> /dev/null; then
    echo "Installing Node.js..."
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
    apt-get install -y nodejs git
fi

# 2. Setup app directory
mkdir -p /opt/gymopus-adms
cd /opt/gymopus-adms
rm -rf *
git clone https://github.com/akshaykumarbgs772-debug/gymopus-adms.git .

# 3. Create 24/7 systemd background service on Port 80
cat << 'SERVICE' > /etc/systemd/system/gymadms.service
[Unit]
Description=GymOpus ADMS Biometric Server
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/gymopus-adms
ExecStart=/usr/bin/node /opt/gymopus-adms/server.js
Restart=always
RestartSec=5
Environment=PORT=80
Environment=FIREBASE_PROJECT_ID=prakash-dry-clean

[Install]
WantedBy=multi-user.target
SERVICE

# 4. Start the service
systemctl daemon-reload
systemctl enable gymadms
systemctl restart gymadms

echo ""
echo "✓ GymOpus ADMS Biometric Server is 100% LIVE on Port 80!"
echo ""
curl -s http://127.0.0.1/health
echo ""
