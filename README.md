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
