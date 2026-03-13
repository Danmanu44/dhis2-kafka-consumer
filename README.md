# DHIS2 Kafka Consumer

A lightweight Kafka consumer that ingests messages and sends them to DHIS2 using the DataValueSets API.

This tool is designed for **health data streaming pipelines** where data is produced to Kafka and ingested into **DHIS2**.

---

## Overview

Many health systems produce data from EMRs, laboratory systems, surveillance systems, and logistics platforms. Instead of batch uploads, this tool enables **real-time streaming ingestion** of data into DHIS2 using Kafka.

Architecture:

Producer → Kafka → Consumer → DHIS2 API

Kafka messages are consumed, sent to the DHIS2 API, and offsets are committed only after successful ingestion.

---

## Features

- Kafka consumer using **confluent-kafka**
- Supports **SASL authentication**
- Sends payloads to the DHIS2 `dataValueSets` API
- Automatic retry on request failure
- Offset commit after successful processing
- Message header logging
- Configurable using YAML
- Docker support for easy deployment

---

## Project Structure

```
dhis2-kafka-consumer
│
├── src/
│   └── consumer.py
│
├── config/
│   └── config.yaml
│
├── examples/
│
├── logs/
│
├── requirements.txt
├── dockerfile
├── README.md
└── .gitignore
```

---

## Requirements

- Python 3.9+
- Kafka cluster
- DHIS2 instance
- Network access to both Kafka and DHIS2 API

---

## Installation

Clone the repository:

```
git clone https://github.com/YOUR_USERNAME/dhis2-kafka-consumer.git
cd dhis2-kafka-consumer
```

Install dependencies:

```
pip install -r requirements.txt
```

---

## Configuration

Edit the configuration file:

```
config/config.yaml
```

Example configuration:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"
  topic: "dhis-topic"
  group_id: "dhis-consumer-group"

  security_protocol: "SASL_PLAINTEXT"
  sasl_mechanism: "PLAIN"
  sasl_username: "sasluser"
  sasl_password: "saslpass"

  session_timeout_ms: 45000
  heartbeat_interval_ms: 15000
  max_poll_interval_ms: 600000

dhis:
  url: "https://your-dhis-instance/api/dataValueSets?async=true"
  username: "username"
  password: "password"

consumer:
  retry_delay: 5
  request_timeout: 60
```

---

## Running the Consumer

Start the consumer:

```
python src/consumer.py
```

If the connection is successful, the consumer will start polling Kafka and sending messages to DHIS2.

Example startup log:

```
Kafka consumer started. Waiting for messages...
```

---

## Kafka Message Format

The consumer expects a **JSON payload compatible with the DHIS2 DataValueSets API**.

Example message:

```json
{
  "dataValues": [
    {
      "dataElement": "abc123",
      "period": "202501",
      "orgUnit": "xyz456",
      "value": "12"
    }
  ]
}
```

Optional Kafka headers can also be attached and will be logged by the consumer.

Example headers:

```
facilityName: Yola Specialist Hospital
datimId: DPA6BxE9pzQ
backlogDate: 12/03/2026
count: 41
```

---

## Running with Docker

Build the image:

```
docker build -t dhis2-kafka-consumer .
```

Run the container:

```
docker run dhis2-kafka-consumer
```

---

## Logging

The consumer prints useful runtime logs such as:

- Kafka message headers
- DHIS API responses
- offset commit status
- retry attempts

Example output:

```
--- Message Headers ---
facilityName: Yola Specialist Hospital
datimId: DPA6BxE9pzQ
------------------------

DHIS accepted payload
Offset committed
```

---

## Error Handling

The consumer implements simple but reliable failure handling:

- HTTP failures trigger retries
- offsets are committed **only after successful DHIS ingestion**
- failed requests are retried after a configurable delay
- consumer continues polling after transient failures

---

## Security

The consumer supports Kafka authentication using:

- SASL/PLAIN
- SASL_PLAINTEXT protocol

For production environments, using **SASL_SSL** is recommended.

---

## Use Cases

Typical integrations include:

- EMR → Kafka → DHIS2
- Laboratory systems → Kafka → DHIS2
- Surveillance systems → Kafka → DHIS2
- Data warehouse pipelines → Kafka → DHIS2

This tool enables **real-time health data streaming into DHIS2**.

---

## Running as a Systemd Service (Linux)

For production deployments, it is recommended to run the consumer as a **systemd service**. This ensures the consumer automatically starts on boot and restarts if it crashes.

### 1. Create a Service Template

Create the following file:

```
/etc/systemd/system/dhis-consumer@.service
```

Service template:

```
[Unit]
Description=DHIS Kafka Consumer Instance %i
After=network.target

[Service]
Type=simple
User=lamis
WorkingDirectory=/opt/dhis2-kafka-consumer
ExecStart=/usr/bin/python3 /opt/dhis2-kafka-consumer/src/consumer.py
Restart=always
RestartSec=5

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Explanation:

- `%i` allows multiple service instances
- `Restart=always` automatically restarts the consumer
- logs are written to the system journal

---

## Reload Systemd

After creating the service file:

```
sudo systemctl daemon-reload
```

---

## Start a Consumer Instance

Example:

```
sudo systemctl start dhis-consumer@1
```

Check status:

```
sudo systemctl status dhis-consumer@1
```

View logs:

```
journalctl -u dhis-consumer@1 -f
```

---

## Enable Auto Start on Boot

```
sudo systemctl enable dhis-consumer@1
```

---

## Running Multiple Consumer Instances

Kafka allows multiple consumers in the same group to **share partitions automatically**.

You can start multiple instances like this:

```
sudo systemctl start dhis-consumer@1
sudo systemctl start dhis-consumer@2
sudo systemctl start dhis-consumer@3
```

Each instance will join the same consumer group and process messages in parallel.

Enable them on boot:

```
sudo systemctl enable dhis-consumer@1
sudo systemctl enable dhis-consumer@2
sudo systemctl enable dhis-consumer@3
```

---

## Stop an Instance

```
sudo systemctl stop dhis-consumer@2
```

---

## Restart an Instance

```
sudo systemctl restart dhis-consumer@1
```

---

## View Logs

Follow logs in real time:

```
journalctl -u dhis-consumer@1 -f
```

Show last 100 logs:

```
journalctl -u dhis-consumer@1 -n 100
```

---

## Scaling Recommendations

For optimal performance:

- number of consumer instances should not exceed Kafka topic partitions
- example:

| Kafka Partitions | Recommended Consumers |
| ---------------- | --------------------- |
| 3                | 1–3                   |
| 6                | 1–6                   |
| 12               | 1–12                  |

---

## Example Production Setup

```
Topic partitions: 6
Consumer group: dhis-consumer-group

Running instances:
dhis-consumer@1
dhis-consumer@2
dhis-consumer@3
```

Each instance processes messages independently while Kafka handles partition balancing automatically.

---

## Monitoring Consumers

Check all running instances:

```
systemctl list-units | grep dhis-consumer
```

Example output:

```
dhis-consumer@1.service
dhis-consumer@2.service
dhis-consumer@3.service
```

---

This setup allows the consumer to run reliably in production environments with automatic restarts and horizontal scaling.

## Roadmap

Possible future improvements:

- parallel worker processing
- dead letter queue for failed messages
- Prometheus metrics
- Kubernetes Helm deployment
- retry backoff strategies
- message validation

---

## Contributing

Contributions are welcome.

You can help by:

- improving documentation
- adding features
- fixing bugs
- improving reliability

Please open an issue or submit a pull request.

---

## License

MIT License

You are free to use, modify, and distribute this project.
