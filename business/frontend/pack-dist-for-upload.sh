#!/bin/bash
# 本地已执行 VITE_BASE=/business/realtime_mv/ npm run build 后，打包 dist 便于上传
# 用法：在项目根目录执行： bash business/frontend/pack-dist-for-upload.sh
# 得到 business/frontend/dist-portal.tar.gz，上传后： cd business/frontend && tar -xzvf dist-portal.tar.gz

set -e
cd "$(dirname "$0")"
if [ ! -d dist ]; then
  echo "请先在本地执行: VITE_BASE=/business/realtime_mv/ npm run build"
  exit 1
fi
tar -czvf dist-portal.tar.gz dist
echo "已生成 dist-portal.tar.gz，上传此文件到服务器后解压到 business/frontend/ 即可。"
