"""
Data models for the Kafka validator.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from enum import Enum


class TableType(str, Enum):
    """Types of tables in the Flink project."""
    SOURCE = "source"
    INTERMEDIATE = "intermediate"
    FACT = "fact"
    DIMENSION = "dimension"


class OutputFormat(str, Enum):
    """Output format options."""
    PRETTY = "pretty"
    JSON = "json"
    COMPACT = "compact"


@dataclass
class TableInfo:
    """Information about a Flink table."""
    table_name: str
    product_name: str
    type: str
    ddl_path: str
    dml_path: Optional[str]
    table_folder: str


@dataclass
class ColumnDef:
    """Column definition from DDL."""
    column_name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False


@dataclass
class AvroConfig:
    """Avro configuration extracted from DDL."""
    key_format: str
    value_format: str
    key_schema_context: Optional[str] = None
    value_schema_context: Optional[str] = None


@dataclass
class ConsumedRecord:
    """A record consumed from Kafka."""
    offset: int
    partition: int
    timestamp: int
    key: Optional[Dict[str, Any]]
    value: Optional[Dict[str, Any]]
    headers: Optional[Dict[str, str]] = None


@dataclass
class ValidationResult:
    """Result of validating a single record."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]


@dataclass
class ValidationStats:
    """Statistics from validation run."""
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    errors_by_field: Dict[str, int] = None
    duration_seconds: float = 0.0
    
    def __post_init__(self):
        if self.errors_by_field is None:
            self.errors_by_field = {}
    
    @property
    def validation_rate(self) -> float:
        """Calculate validation success rate."""
        if self.total_records == 0:
            return 0.0
        return (self.valid_records / self.total_records) * 100

