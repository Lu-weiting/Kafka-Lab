# Kafka Example for NodeJS and Python3

This is a simple example of how to use Kafka with NodeJS and Python3.

## Prerequisites

- Python 3 (3.8 best)
- Docker
- NodeJS

## How to run

Run Kafka broker locally:
```bash
sudo docker-compose up -d
```

Install Python dependencies:
```bash
pip install -r requirements.txt
```
Install NodeJS dependencies:
```bash
npm i
```

Start producer:
```bash
node producer.js
```

Start consumer:
```bash
node consumer.js
```
or
```bash
python3 consumer.py
```