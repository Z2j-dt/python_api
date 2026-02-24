#!/bin/bash
# 在服务器上执行：只同步前端源码（不传 dist），在服务器上构建，省去大文件传输。
# 用法：把整个 python_api 同步到服务器后，在项目根目录执行：
#   cd business/frontend && bash build-on-server.sh

set -e
echo "Installing dependencies..."
npm install
echo "Building for portal (base /business/realtime_mv/)..."
VITE_BASE=/business/realtime_mv/ npm run build
echo "Done. dist/ is ready for the portal."
