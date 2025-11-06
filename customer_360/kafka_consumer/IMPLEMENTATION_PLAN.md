# Kafka Consumer with Avro Schema Validation - Implementation Plan

## Overview

This document outlines the implementation plan for a generic Kafka consumer that validates records against Avro schemas for any Flink table in the C360 project.

**Usage**: `python validate_records.py src_c360_products [options]`

## Architecture

Use uv for project management
### Components

```
┌─────────────────────────────────────────────────────────────────┐
│                       validate_records.py                        │
│                         (CLI Entry Point)                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ├──► ConfigManager
                         │    - Load config.yaml and environment variables for secrets and kafka url. config.yaml is defined in CONFIG_FILE env variables
                         │    - Extract Kafka/SR credentials
                         │    - Build connection configs
                         │
                         ├──► TableResolver
                         │    - Load inventory.json
                         │    - Resolve table name to folder
                         │    - Locate DDL file
                         │
                         ├──► DDLParser
                         │    - Parse Flink SQL DDL
                         │    - Extract table schema
                         │    - Extract Kafka topic name
                         │    - Extract Avro format config
                         │
                         ├──► SchemaRegistryClient
                         │    - Connect to Schema Registry
                         │    - Fetch key/value schemas
                         │    - Handle schema contexts
                         │
                         ├──► AvroKafkaConsumer
                         │    - Create Kafka consumer
                         │    - Configure Avro deserializers
                         │    - Consume and deserialize records
                         │
                         └──► RecordValidator
                              - Validate deserialized records
                              - Track validation metrics
                              - Report statistics
```

## Component Details

### 1. CLI Entry Point (`validate_records.py`)

**Purpose**: Main entry point with CLI interface

**Dependencies**:
- `typer` - CLI framework
- `rich` - Beautiful console output
- `pyyaml` - Configuration parsing

**Interface**:
```python
@click.command()
@click.argument('table_name')
@click.option('--config', '-c', default='config.yaml', help='Path to config file')
@click.option('--project-root', '-p', default='.', help='Project root directory')
@click.option('--max-records', '-n', default=10, help='Maximum records to consume')
@click.option('--timeout', '-t', default=30, help='Timeout in seconds')
@click.option('--output-format', '-f', type=typer.Choice(['pretty', 'json', 'compact']), default='pretty')
@click.option('--validate-schema/--no-validate-schema', default=True, help='Enable schema validation')
@click.option('--from-beginning/--from-latest', default=False, help='Start from beginning or latest')
@click.option('--group-id', '-g', default=None, help='Consumer group ID (auto-generated if not provided)')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
def validate_records(table_name, config, project_root, max_records, timeout, 
                    output_format, validate_schema, from_beginning, group_id, verbose):
    """Validate Kafka records for a Flink table using Avro schema validation."""
    pass
```

**Output Examples**:

*Pretty Format*:
```
╭─────────────────────────────────────────╮
│ Validating: src_c360_products           │
│ Topic: src_c360_products                │
│ Schema Context: .flink-dev              │
╰─────────────────────────────────────────╯

✓ Record 1/10 [offset: 42]
  product_id: "PROD-001"
  product_name: "Wireless Mouse"
  category: "Electronics"
  price: 29.99

✓ Record 2/10 [offset: 43]
  ...

╭─────────────── Summary ──────────────────╮
│ Total Records: 10                        │
│ Valid Records: 10 (100%)                 │
│ Invalid Records: 0 (0%)                  │
│ Duration: 2.3s                           │
╰──────────────────────────────────────────╯
```

*JSON Format*:
```json
{
  "table_name": "src_c360_products",
  "topic": "src_c360_products",
  "records": [...],
  "summary": {
    "total": 10,
    "valid": 10,
    "invalid": 0,
    "duration_seconds": 2.3
  }
}
```

### 2. ConfigManager

**Purpose**: Load and manage configuration from `config.yaml`

**Key Methods**:
```python
class ConfigManager:
    def __init__(self, config_path: str):
        """Load configuration from YAML file."""
        
    def get_kafka_config(self) -> Dict[str, str]:
        """Return Kafka consumer configuration."""
        return {
            'bootstrap.servers': self.config['kafka']['bootstrap.servers'],
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': self._get_kafka_api_key(),
            'sasl.password': self._get_kafka_api_secret(),
            'group.id': f'validator-{uuid.uuid4()}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
    
    def get_schema_registry_config(self) -> Dict[str, str]:
        """Return Schema Registry configuration."""
        return {
            'url': self._build_schema_registry_url(),
            'basic.auth.user.info': f'{self._get_sr_api_key()}:{self._get_sr_api_secret()}'
        }
    
    def _build_schema_registry_url(self) -> str:
        """Build Schema Registry URL for Confluent Cloud."""
        env_id = self.config['confluent_cloud']['environment_id']
        region = self.config['confluent_cloud']['region']
        provider = self.config['confluent_cloud']['provider']
        return f'https://psrc-{region}.{provider}.confluent.cloud'
```

**Configuration Mapping**:
- Extract Kafka bootstrap servers
- Build Schema Registry URL from environment/region/provider
- Handle authentication credentials (API keys from environment variables or config)
- Support both local and Confluent Cloud deployments

### 3. TableResolver

**Purpose**: Resolve table name to DDL file location using inventory.json

**Key Methods**:
```python
class TableResolver:
    def __init__(self, project_root: str):
        """Load inventory.json from project."""
        
    def resolve_table(self, table_name: str) -> TableInfo:
        """
        Resolve table name to its metadata.
        
        Returns:
            TableInfo containing:
            - table_name: str
            - product_name: str
            - type: str (source, intermediate, fact)
            - ddl_path: str
            - dml_path: str
            - table_folder: str
        """
        
    def get_ddl_path(self, table_name: str) -> Path:
        """Get absolute path to table's DDL file."""
        
    def list_all_tables(self) -> List[str]:
        """Return list of all available table names."""
```

**Example**:
```python
resolver = TableResolver('/path/to/c360_flink_processing')
info = resolver.resolve_table('src_c360_products')
# Returns:
# TableInfo(
#   table_name='src_c360_products',
#   product_name='c360',
#   type='source',
#   ddl_path='pipelines/sources/c360/src_products/sql-scripts/ddl.src_c360_products.sql',
#   ...
# )
```

### 4. DDLParser

**Purpose**: Parse Flink SQL DDL to extract schema and configuration

**Key Methods**:
```python
class DDLParser:
    def __init__(self, ddl_path: str):
        """Load and parse DDL file."""
        
    def get_topic_name(self) -> str:
        """
        Extract topic name from DDL.
        For tables without explicit topic in WITH clause,
        topic name = table name.
        """
        
    def get_schema(self) -> List[ColumnDef]:
        """
        Parse column definitions.
        
        Returns list of:
        - column_name: str
        - data_type: str (Flink SQL type)
        - nullable: bool
        """
        
    def get_primary_key(self) -> List[str]:
        """Extract primary key column names."""
        
    def get_kafka_config(self) -> Dict[str, str]:
        """Extract all WITH clause properties."""
        
    def get_avro_format_config(self) -> AvroConfig:
        """
        Extract Avro-specific configuration.
        
        Returns:
        - key_format: str ('avro-registry')
        - value_format: str ('avro-registry')
        - key_schema_context: str (e.g., '.flink-dev')
        - value_schema_context: str
        """
```

**DDL Parsing Logic**:

Example DDL:
```sql
CREATE TABLE IF NOT EXISTS src_c360_products (
    product_id STRING,
    product_name STRING,
    price DECIMAL(10, 2),
    PRIMARY KEY(product_id) NOT ENFORCED
) WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev'
);
```

Parsed Output:
```python
{
  'topic_name': 'src_c360_products',
  'schema': [
    ColumnDef('product_id', 'STRING', nullable=False),
    ColumnDef('product_name', 'STRING', nullable=True),
    ColumnDef('price', 'DECIMAL(10,2)', nullable=True),
  ],
  'primary_key': ['product_id'],
  'avro_config': {
    'key_format': 'avro-registry',
    'value_format': 'avro-registry',
    'key_schema_context': '.flink-dev',
    'value_schema_context': '.flink-dev'
  }
}
```

### 5. SchemaRegistryClient Wrapper

**Purpose**: Fetch schemas from Confluent Schema Registry

**Key Methods**:
```python
class SchemaRegistryManager:
    def __init__(self, config: Dict[str, str]):
        """Initialize Schema Registry client."""
        from confluent_kafka.schema_registry import SchemaRegistryClient
        self.client = SchemaRegistryClient(config)
        
    def get_latest_schema(self, subject: str) -> Schema:
        """
        Get latest schema for a subject.
        
        Subject naming:
        - Key: {topic}-key:{context}
        - Value: {topic}-value:{context}
        
        Example: src_c360_products-value:.flink-dev
        """
        
    def get_key_schema(self, topic: str, context: str = None) -> Schema:
        """Get key schema for topic."""
        subject = f'{topic}-key'
        if context:
            subject += f':{context}'
        return self.get_latest_schema(subject)
        
    def get_value_schema(self, topic: str, context: str = None) -> Schema:
        """Get value schema for topic."""
        subject = f'{topic}-value'
        if context:
            subject += f':{context}'
        return self.get_latest_schema(subject)
        
    def list_subjects(self, prefix: str = None) -> List[str]:
        """List all subjects, optionally filtered by prefix."""
```

**Schema Registry Integration**:
- Connect to Confluent Cloud Schema Registry
- Handle authentication
- Support schema contexts (e.g., `.flink-dev`)
- Fetch both key and value schemas
- Handle schema versioning

### 6. AvroKafkaConsumer

**Purpose**: Consume Kafka messages with Avro deserialization

**Key Methods**:
```python
class AvroKafkaConsumer:
    def __init__(self, 
                 kafka_config: Dict[str, str],
                 schema_registry_config: Dict[str, str],
                 topic: str,
                 key_schema: Schema = None,
                 value_schema: Schema = None):
        """Initialize consumer with Avro deserializers."""
        
    def consume(self, 
                max_records: int = 10,
                timeout_seconds: int = 30) -> Iterator[ConsumedRecord]:
        """
        Consume records from Kafka topic.
        
        Yields:
            ConsumedRecord with:
            - offset: int
            - partition: int
            - timestamp: int
            - key: Dict (deserialized)
            - value: Dict (deserialized)
            - headers: Dict
        """
        
    def close(self):
        """Close consumer and cleanup resources."""
```

**Implementation**:
```python
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

class AvroKafkaConsumer:
    def __init__(self, kafka_config, schema_registry_config, topic, 
                 key_schema=None, value_schema=None):
        # Create Schema Registry client
        sr_client = SchemaRegistryClient(schema_registry_config)
        
        # Create deserializers
        self.key_deserializer = AvroDeserializer(sr_client, schema_str=key_schema) if key_schema else None
        self.value_deserializer = AvroDeserializer(sr_client, schema_str=value_schema) if value_schema else None
        
        # Create consumer
        consumer_config = kafka_config.copy()
        consumer_config['key.deserializer'] = self.key_deserializer
        consumer_config['value.deserializer'] = self.value_deserializer
        
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([topic])
        self.topic = topic
        
    def consume(self, max_records=10, timeout_seconds=30):
        """Consume and yield deserialized records."""
        start_time = time.time()
        record_count = 0
        
        while record_count < max_records:
            if time.time() - start_time > timeout_seconds:
                break
                
            msg = self.consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                # Handle errors
                continue
                
            # Message successfully consumed and deserialized
            yield ConsumedRecord(
                offset=msg.offset(),
                partition=msg.partition(),
                timestamp=msg.timestamp()[1],
                key=msg.key(),
                value=msg.value(),
                headers=msg.headers()
            )
            record_count += 1
```

### 7. RecordValidator

**Purpose**: Validate records and track metrics

**Key Methods**:
```python
class RecordValidator:
    def __init__(self, expected_schema: List[ColumnDef]):
        """Initialize validator with expected schema."""
        self.expected_schema = expected_schema
        self.stats = ValidationStats()
        
    def validate_record(self, record: Dict) -> ValidationResult:
        """
        Validate a single record.
        
        Checks:
        - All required fields present
        - Data types match expected types
        - Nullable constraints
        - Primary key not null
        
        Returns:
            ValidationResult with:
            - is_valid: bool
            - errors: List[str]
            - warnings: List[str]
        """
        
    def get_statistics(self) -> ValidationStats:
        """
        Return validation statistics.
        
        Stats:
        - total_records: int
        - valid_records: int
        - invalid_records: int
        - errors_by_field: Dict[str, int]
        - validation_rate: float
        """
```

## Project Structure

```
kafka_consumer/
├── README.md                    # User documentation
├── IMPLEMENTATION_PLAN.md       # This file
├── requirements.txt             # Python dependencies
├── pyproject.toml              # Project metadata (using uv)
├── validate_records.py         # CLI entry point
├── src/
│   └── kafka_validator/
│       ├── __init__.py
│       ├── config.py           # ConfigManager
│       ├── resolver.py         # TableResolver
│       ├── ddl_parser.py       # DDLParser
│       ├── schema_registry.py  # SchemaRegistryManager
│       ├── consumer.py         # AvroKafkaConsumer
│       ├── validator.py        # RecordValidator
│       ├── models.py           # Data models (TableInfo, ColumnDef, etc.)
│       └── formatters.py       # Output formatters (pretty, json, compact)
├── tests/
│   ├── __init__.py
│   ├── test_config.py
│   ├── test_ddl_parser.py
│   ├── test_validator.py
│   └── fixtures/
│       ├── sample_ddl.sql
│       ├── sample_config.yaml
│       └── sample_inventory.json
└── examples/
    ├── basic_validation.sh
    ├── json_output.sh
    └── batch_validation.sh
```

## Dependencies

**Core Dependencies** (`requirements.txt`):
```txt
# Kafka and Avro
confluent-kafka[avro,schema-registry]==2.3.0

# CLI
click==8.1.7
rich==13.7.0

# Config and parsing
pyyaml==6.0.1
python-dotenv==1.0.0

# Data models
pydantic==2.5.0

# Optional: for testing
pytest==7.4.3
pytest-mock==3.12.0
```

**Using uv** (`pyproject.toml`):
```toml
[project]
name = "kafka-validator"
version = "0.1.0"
description = "Generic Kafka consumer with Avro schema validation for Flink tables"
requires-python = ">=3.9"
dependencies = [
    "confluent-kafka[avro,schema-registry]>=2.3.0",
    "click>=8.1.7",
    "rich>=13.7.0",
    "pyyaml>=6.0.1",
    "python-dotenv>=1.0.0",
    "pydantic>=2.5.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.3",
    "pytest-mock>=3.12.0",
    "black>=23.12.0",
    "ruff>=0.1.7",
]

[project.scripts]
validate-records = "kafka_validator.cli:main"
```

## Implementation Steps

### Phase 1: Core Infrastructure (Tasks 1-4)
1. **Project Setup**
   - Create directory structure
   - Set up `pyproject.toml` with dependencies
   - Initialize git
   - Create basic README

2. **Configuration Management**
   - Implement `ConfigManager` class
   - Load and parse `config.yaml`
   - Build Kafka and Schema Registry configs
   - Handle environment variables for secrets

3. **Table Resolution**
   - Implement `TableResolver` class
   - Load `inventory.json`
   - Resolve table names to DDL paths

4. **DDL Parsing**
   - Implement `DDLParser` class
   - Parse CREATE TABLE statements
   - Extract column definitions
   - Parse WITH clause properties
   - Extract Avro configuration

### Phase 2: Kafka Integration (Tasks 5-6)
5. **Schema Registry Integration**
   - Implement `SchemaRegistryManager` class
   - Connect to Confluent Cloud Schema Registry
   - Fetch schemas by subject
   - Handle schema contexts

6. **Kafka Consumer**
   - Implement `AvroKafkaConsumer` class
   - Configure Avro deserializers
   - Implement consume loop
   - Handle errors and timeouts

### Phase 3: Validation & Output (Tasks 7-9)
7. **Record Validation**
   - Implement `RecordValidator` class
   - Validate field presence
   - Validate data types
   - Track validation metrics

8. **CLI Interface**
   - Implement main `validate_records.py`
   - Add CLI options with Click
   - Wire up all components
   - Handle errors gracefully

9. **Output Formatting**
   - Implement pretty formatter (using Rich)
   - Implement JSON formatter
   - Implement compact formatter
   - Add summary statistics

### Phase 4: Testing & Documentation (Tasks 10-11)
10. **Documentation**
    - Write comprehensive README
    - Add usage examples
    - Document configuration options
    - Create troubleshooting guide

11. **Testing**
    - Unit tests for DDL parser
    - Unit tests for validator
    - Integration test example
    - Add test fixtures

## Usage Examples

### Basic Usage
```bash
# Validate 10 records from src_c360_products
python validate_records.py src_c360_products

# Validate 50 records with JSON output
python validate_records.py src_c360_products -n 50 -f json > output.json

# Start from beginning and validate 100 records
python validate_records.py src_c360_products --from-beginning -n 100
```

### Advanced Usage
```bash
# Validate with custom config file
python validate_records.py src_c360_products -c /path/to/config.yaml

# Validate with custom project root
python validate_records.py src_c360_products -p /path/to/c360_flink_processing

# Specify consumer group ID (useful for resuming)
python validate_records.py src_c360_products -g my-validator-group

# Verbose output for debugging
python validate_records.py src_c360_products -v

# Disable schema validation (just consume and display)
python validate_records.py src_c360_products --no-validate-schema
```

### Batch Validation Script
```bash
#!/bin/bash
# Validate all source tables

TABLES=(
  "src_c360_customers"
  "src_c360_products"
  "src_c360_transactions"
  "src_c360_tx_items"
  "src_c360_app_usage"
  "src_c360_support_ticket"
  "src_c360_loyalty_program"
)

for table in "${TABLES[@]}"; do
  echo "Validating $table..."
  python validate_records.py "$table" -n 20 -f json > "results/${table}.json"
  echo "✓ Done"
done
```

## Configuration

### Environment Variables
```bash
# Kafka API credentials
export KAFKA_API_KEY="your-kafka-api-key"
export KAFKA_API_SECRET="your-kafka-api-secret"

# Schema Registry API credentials
export SCHEMA_REGISTRY_API_KEY="your-sr-api-key"
export SCHEMA_REGISTRY_API_SECRET="your-sr-api-secret"

# Optional: Custom config path
export KAFKA_VALIDATOR_CONFIG="/path/to/config.yaml"
```

### Config File Structure
The tool uses the existing `config.yaml` format:
```yaml
kafka:
  bootstrap.servers: pkc-xxxxx.us-west-2.aws.confluent.cloud:9092
  cluster_id: lkc-xxxxx
  
confluent_cloud:
  environment_id: env-xxxxx
  region: us-west-2
  provider: aws
  
flink:
  catalog_name: j9r-env
  database_name: j9r-kafka
```

## Error Handling

### Common Scenarios

1. **Table Not Found**
   ```
   Error: Table 'src_c360_invalid' not found in inventory.
   Available tables:
     - src_c360_customers
     - src_c360_products
     ...
   ```

2. **Schema Not Found in Registry**
   ```
   Error: Schema not found in Schema Registry
   Subject: src_c360_products-value:.flink-dev
   Hint: Ensure the table has been deployed and data has been produced.
   ```

3. **Deserialization Error**
   ```
   Warning: Failed to deserialize record at offset 42
   Error: Schema mismatch - field 'price' expected DECIMAL but got STRING
   ```

4. **Connection Timeout**
   ```
   Error: Timeout waiting for records
   Topic: src_c360_products
   Duration: 30s
   Hint: Check if topic has data and Kafka connectivity.
   ```

## Testing Strategy

### Unit Tests
- **ConfigManager**: Test YAML parsing, config building
- **TableResolver**: Test inventory loading, table resolution
- **DDLParser**: Test SQL parsing, schema extraction
- **RecordValidator**: Test validation logic

### Integration Tests
- **End-to-End**: Test against actual Kafka topic (using test topic)
- **Mock Integration**: Use mock consumer for CI/CD

### Test Fixtures
- Sample DDL files for various table types
- Sample config files
- Mock Kafka messages
- Mock Schema Registry responses

## Future Enhancements

1. **Performance**
   - Parallel validation for multiple tables
   - Batch validation mode
   - Stream processing for large datasets

2. **Validation Rules**
   - Custom validation rules per table
   - Business logic validation
   - Cross-field validation

3. **Reporting**
   - HTML reports
   - Prometheus metrics export
   - Dashboard integration

4. **Integration**
   - Integration with shift_left CLI
   - Integration with CI/CD pipelines
   - Automated regression testing

5. **Advanced Features**
   - Compare records between environments
   - Schema evolution validation
   - Data profiling and statistics

## Success Criteria

- [ ] Can validate records for any table in inventory
- [ ] Properly deserializes Avro messages from Schema Registry
- [ ] Provides clear, actionable error messages
- [ ] Handles missing topics/schemas gracefully
- [ ] Supports both pretty and JSON output
- [ ] Comprehensive documentation
- [ ] Unit test coverage > 80%
- [ ] Integration test for end-to-end flow

## Appendix

### Confluent Cloud Schema Registry URLs

Format: `https://psrc-{region}.{provider}.confluent.cloud`

Examples:
- AWS us-west-2: `https://psrc-us-west-2.aws.confluent.cloud`
- GCP us-central1: `https://psrc-us-central1.gcp.confluent.cloud`
- Azure eastus: `https://psrc-eastus.azure.confluent.cloud`

### Schema Subject Naming

For tables with schema context:
- Key subject: `{topic-name}-key:{context}`
- Value subject: `{topic-name}-value:{context}`

Example:
- Key: `src_c360_products-key:.flink-dev`
- Value: `src_c360_products-value:.flink-dev`

Without context:
- Key: `{topic-name}-key`
- Value: `{topic-name}-value`

### Flink SQL to Avro Type Mapping

| Flink SQL Type | Avro Type | Python Type |
|---------------|-----------|-------------|
| STRING | string | str |
| INT | int | int |
| BIGINT | long | int |
| FLOAT | float | float |
| DOUBLE | double | float |
| BOOLEAN | boolean | bool |
| DECIMAL(p,s) | bytes (logical decimal) | Decimal |
| DATE | int (logical date) | date |
| TIMESTAMP(3) | long (logical timestamp-millis) | datetime |
| ARRAY<T> | array | list |
| MAP<K,V> | map | dict |
| ROW<...> | record | dict |

