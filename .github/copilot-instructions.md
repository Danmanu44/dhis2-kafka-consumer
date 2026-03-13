# Copilot Instructions for dhis-kafka-consumer

## Project Overview
This project is a Kafka consumer that reads messages from a Kafka topic and forwards them to a DHIS2 API endpoint. It is designed for robust, production-grade message processing with explicit handling of message offsets, errors, and retries.

## Architecture & Data Flow
- **Kafka Consumer**: Configured via `config/config.yaml` and instantiated in `src/consumer.py`. Uses SASL authentication and custom poll/session settings for reliability.
- **DHIS Integration**: Messages are sent to DHIS2 via HTTP POST, with basic authentication. The endpoint and credentials are set in `config/config.yaml`.
- **Config Management**: All runtime parameters (Kafka, DHIS, consumer) are loaded from `config/config.yaml`.
- **Error Handling**: Consumer handles null/tombstone messages, Kafka errors, and DHIS API failures. Failed DHIS requests are retried after a configurable delay.

## Developer Workflows
- **Run Consumer**: Execute `python src/consumer.py` after installing dependencies from `requirements.txt`.
- **Configuration**: Edit `config/config.yaml` for Kafka/DHIS settings. Do not hardcode credentials or endpoints in code.
- **Dependencies**: Install with `pip install -r requirements.txt`. Key packages: `confluent-kafka`, `requests`, `pyyaml`.
- **Debugging**: Print statements are used for runtime diagnostics. Logs are not persisted by default; add file logging if needed.

## Project Conventions
- **Single Consumer Script**: All logic is in `src/consumer.py`. No modularization or test suite is present.
- **Explicit Commit**: Offsets are committed only after successful DHIS API response.
- **Header Extraction**: Kafka message headers are decoded and printed for traceability.
- **Retry Logic**: Failed DHIS requests are retried after a delay (default: 5 seconds).

## Integration Points
- **Kafka**: Uses SASL_PLAINTEXT with credentials from config.
- **DHIS2 API**: Basic Auth, JSON payloads, endpoint from config.

## Examples & Patterns
- **Config Example**: See `config/config.yaml` for all tunable parameters.
- **Consumer Example**: See `src/consumer.py` for message loop, error handling, and DHIS integration.

## Recommendations for AI Agents
- Always read `config/config.yaml` for runtime settings.
- Preserve explicit error handling and retry logic.
- When adding features, keep all configuration in YAML, not code.
- If modularizing, reference `src/consumer.py` for main patterns.
- Document any new developer workflow or integration in this file.

---
_Last updated: March 13, 2026_
