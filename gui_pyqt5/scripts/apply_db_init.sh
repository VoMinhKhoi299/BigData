#!/bin/bash
# Dừng nếu có lỗi
set -e

# Đọc config
HOST=$(grep "host" ../config.ini | cut -d'=' -f2 | xargs)
USER=$(grep "user" ../config.ini | cut -d'=' -f2 | xargs)
PASSWORD=$(grep "password" ../config.ini | cut -d'=' -f2 | xargs)

echo "🚀 Applying database schema to $HOST..."
mysql -h $HOST -u $USER -p$PASSWORD < ../db_init.sql
echo "✅ Database initialized successfully!"
