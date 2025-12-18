# Event Log Demo - Clean Code Quick Reference

## 🚀 Quick Start After Refactoring

### Run Tests
```powershell
# Producer tests
cd c:\dev\workspace\event-log-demo\components\producer
pytest tests/ -v

# Processor tests
cd c:\dev\workspace\event-log-demo\components\processor
pytest tests/ -v
```

### Start Services
```powershell
cd c:\dev\workspace\event-log-demo
docker-compose build
docker-compose up -d
```

---

## 🔒 Security Checklist

### Before Deploying to Production

- [ ] **Set secure credentials** (never use defaults):
  ```bash
  export MINIO_ACCESS_KEY=$(openssl rand -base64 32)
  export MINIO_SECRET_KEY=$(openssl rand -base64 32)
  ```

- [ ] **Verify no security warnings** in logs:
  ```bash
  docker-compose logs | grep "SECURITY WARNING"
  ```

- [ ] **Run all tests**:
  ```bash
  pytest components/*/tests/ -v
  ```

- [ ] **Check for hardcoded secrets**:
  ```bash
  grep -r "minioadmin" components/
  # Should only appear in default values with warnings
  ```

---

## 📦 Environment Variables Reference

### Producer Service
| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Yes | Kafka broker address |
| `CASSANDRA_HOST` | `cassandra` | Yes | Cassandra host |
| `CASSANDRA_KEYSPACE` | `logs` | No | Cassandra keyspace |
| `CASSANDRA_TABLE` | `events` | No | Cassandra table name |
| `MINIO_ENDPOINT` | `http://minio:9000` | Yes | MinIO endpoint URL |
| `MINIO_ACCESS_KEY` | `minioadmin` | **Yes*** | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | **Yes*** | MinIO secret key |
| `MINIO_BUCKET` | `event-logs` | No | MinIO bucket name |

**\*Change in production!**

### Processor Service
| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Yes | Kafka broker address |
| `KAFKA_TOPIC` | `event-logs` | No | Kafka topic name |
| `KAFKA_GROUP_ID` | `flink-processor-group` | No | Kafka consumer group |
| `CASSANDRA_HOST` | `cassandra` | Yes | Cassandra host |
| `CASSANDRA_KEYSPACE` | `logs` | No | Cassandra keyspace |
| `CASSANDRA_TABLE` | `events` | No | Cassandra table name |
| `STARTUP_DELAY_SECONDS` | `10` | No | Startup delay (0-300) |

### Flink Job
| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Yes | Kafka broker address |
| `KAFKA_TOPIC` | `event-logs` | No | Kafka topic name |
| `KAFKA_GROUP_ID` | `flink-processor-group` | No | Kafka consumer group |
| `CASSANDRA_HOST` | `cassandra` | Yes | Cassandra host |
| `CASSANDRA_KEYSPACE` | `logs` | No | Cassandra keyspace |
| `CASSANDRA_TABLE` | `events` | No | Cassandra table name |
| `CASSANDRA_TTL_SECONDS` | `5184000` (60 days) | No | Data retention TTL |
| `MINIO_ENDPOINT` | `http://minio:9000` | Yes | MinIO endpoint URL |
| `MINIO_ACCESS_KEY` | `minioadmin` | **Yes*** | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | **Yes*** | MinIO secret key |
| `MINIO_BUCKET` | `event-logs` | No | MinIO bucket name |
| `FLINK_PARALLELISM` | `4` | No | Parallelism level |

---

## 🧪 Testing Guide

### Unit Tests
Test individual functions in isolation:
```powershell
# Producer value objects
pytest components/producer/tests/test_value_objects.py -v

# Producer bottleneck detector
pytest components/producer/tests/test_bottleneck_detector.py -v

# Processor configuration
pytest components/processor/tests/test_config.py -v
```

### Integration Tests
Test component interactions:
```powershell
# Processor integration
pytest components/processor/tests/test_integration.py -v
```

### Security Tests
Verify OWASP compliance:
```powershell
# Test input validation (OWASP A03)
pytest components/producer/tests/test_value_objects.py::test_negative_rate_rejected -v

# Test configuration security (OWASP A05)
pytest components/processor/tests/test_config.py::test_app_config_immutable -v
```

---

## 🐛 Common Issues

### Issue: "Invalid load test configuration"
**Cause**: Value object validation rejected input  
**Solution**: Check input ranges:
- Rate: 0.1 - 10,000 msg/sec
- Duration: 1 - 3,600 seconds
- Size: 0.1 - 1,024 KB

### Issue: "SECURITY WARNING: Using default MinIO credentials"
**Cause**: Using default credentials in production  
**Solution**: Set environment variables:
```bash
export MINIO_ACCESS_KEY=your-secure-key
export MINIO_SECRET_KEY=your-secure-secret
```

### Issue: "Configuration validation failed"
**Cause**: Empty or invalid environment variable  
**Solution**: Check all required variables are set:
```bash
echo $KAFKA_BOOTSTRAP_SERVERS
echo $CASSANDRA_HOST
```

---

## 📊 Code Quality Metrics

### Complexity Thresholds
- ✅ **Cyclomatic Complexity**: < 5 per function
- ✅ **Function Length**: < 20 lines
- ✅ **Parameter Count**: < 4 parameters

### Test Coverage Goals
- ✅ **Critical Paths**: 100% coverage
- ✅ **Business Logic**: 90%+ coverage
- ✅ **Security Boundaries**: 100% coverage

---

## 🔍 Code Review Checklist

Before merging changes:

- [ ] No hardcoded credentials or secrets
- [ ] All functions have cyclomatic complexity < 5
- [ ] Input validation using value objects
- [ ] Error messages don't expose sensitive data
- [ ] Configuration loaded from environment
- [ ] Tests cover security boundaries
- [ ] No magic numbers (all named constants)
- [ ] Functions follow Single Responsibility Principle

---

## 📚 Further Reading

- **Full Refactoring Report**: [REFACTORING_COMPLETE.md](./REFACTORING_COMPLETE.md)
- **Producer Refactoring Notes**: [components/producer/REFACTORING_NOTES.md](./components/producer/REFACTORING_NOTES.md)
- **OWASP Top 10**: https://owasp.org/www-project-top-ten/
- **SonarQube Rules**: https://rules.sonarsource.com/

---

**Last Updated**: December 18, 2025
