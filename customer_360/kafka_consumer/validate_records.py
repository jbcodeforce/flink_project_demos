#!/usr/bin/env python3
"""
Kafka Record Validator - Main Entry Point

This is the top-level CLI script for validating Kafka records against Avro schemas.

Usage:
    python validate_records.py TABLE_NAME [OPTIONS]
    
    Or install with uv and use:
    validate-records TABLE_NAME [OPTIONS]

Examples:
    python validate_records.py src_c360_products
    python validate_records.py src_c360_products -n 50 -f json
    python validate_records.py src_c360_products --from-beginning -n 100 -v
"""

import sys

# Import the main CLI function from the package
from src.kafka_validator.cli import main

if __name__ == "__main__":
    sys.exit(main())

