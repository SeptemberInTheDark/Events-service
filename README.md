##Back api

python3 -m venv venv

source venv/bin/activate

sudo chmod +x ./dev.sh

sudo nano /etc/hosts
127.0.0.1 kafka clickhouse

./dev.sh