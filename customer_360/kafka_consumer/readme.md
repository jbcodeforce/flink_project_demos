# Kafka Consumer to Validate Messages in Kafka Topics

A generic Kafka consumer with Avro schema validation for Flink tables in the C360 project.

## Status

📋 **Implementation Plan Ready** - See [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) for detailed architecture and implementation guide.

## Quick Start

Once implemented, usage will be:

```bash
# Validate records from a Flink table
python validate_records.py src_c360_products

# Validate 50 records with JSON output
python validate_records.py src_c360_products -n 50 -f json

# Start from beginning
python validate_records.py src_c360_products --from-beginning -n 100
```

## Features

- ✅ **Generic**: Works with any table in the Flink project inventory
- ✅ **Avro Schema Validation**: Validates records against Schema Registry schemas
- ✅ **Multiple Output Formats**: Pretty console output, JSON, or compact format
- ✅ **Flexible**: Configurable record count, timeout, starting offset
- ✅ **Schema Context Support**: Handles Confluent Cloud schema contexts (e.g., `.flink-dev`)

## Implementation Plan

See [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) for:
- Complete architecture diagram
- Component details and interfaces
- Implementation steps (11 tasks)
- Usage examples
- Testing strategy
- Future enhancements

## Next Steps

To implement this tool:

1. Follow Phase 1 (Core Infrastructure) in the implementation plan
2. Set up project structure with `pyproject.toml`
3. Implement core components (ConfigManager, TableResolver, DDLParser)
4. Continue with Phases 2-4 for full functionality