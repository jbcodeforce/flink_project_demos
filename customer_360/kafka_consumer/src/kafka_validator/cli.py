"""
CLI entry point for Kafka record validation.

Usage: validate-records TABLE_NAME [OPTIONS]
"""

import sys
import time
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table as RichTable

from .models import OutputFormat

console = Console()


@click.command()
@click.argument('table_name')
@click.option(
    '--config', '-c',
    default=None,
    type=click.Path(exists=True),
    help='Path to config file (default: uses CONFIG_FILE env variable)'
)
@click.option(
    '--project-root', '-p',
    default='.',
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help='Project root directory containing Flink pipelines'
)
@click.option(
    '--max-records', '-n',
    default=10,
    type=int,
    help='Maximum records to consume'
)
@click.option(
    '--timeout', '-t',
    default=30,
    type=int,
    help='Timeout in seconds'
)
@click.option(
    '--output-format', '-f',
    type=click.Choice(['pretty', 'json', 'compact'], case_sensitive=False),
    default='pretty',
    help='Output format'
)
@click.option(
    '--validate-schema/--no-validate-schema',
    default=True,
    help='Enable/disable schema validation'
)
@click.option(
    '--from-beginning/--from-latest',
    default=False,
    help='Start consuming from beginning or latest offset'
)
@click.option(
    '--group-id', '-g',
    default=None,
    help='Consumer group ID (auto-generated if not provided)'
)
@click.option(
    '--verbose', '-v',
    is_flag=True,
    help='Verbose output for debugging'
)
def main(
    table_name: str,
    config: Optional[str],
    project_root: str,
    max_records: int,
    timeout: int,
    output_format: str,
    validate_schema: bool,
    from_beginning: bool,
    group_id: Optional[str],
    verbose: bool
):
    """
    Validate Kafka records for a Flink table using Avro schema validation.
    
    TABLE_NAME: Name of the Flink table to validate (e.g., src_c360_products)
    
    Examples:
    
        # Validate 10 records from src_c360_products
        validate-records src_c360_products
        
        # Validate 50 records with JSON output
        validate-records src_c360_products -n 50 -f json > output.json
        
        # Start from beginning and validate 100 records
        validate-records src_c360_products --from-beginning -n 100
    """
    start_time = time.time()
    
    try:
        # Display header
        if output_format == 'pretty':
            _display_header(table_name, verbose)
        
        # TODO: Initialize components
        # 1. Load configuration
        # 2. Resolve table to DDL file
        # 3. Parse DDL
        # 4. Connect to Schema Registry
        # 5. Create Kafka consumer
        # 6. Consume and validate records
        # 7. Display results
        
        # Placeholder for now
        if output_format == 'pretty':
            console.print("\n[yellow]⚠ Implementation in progress...[/yellow]")
            console.print(f"\n[cyan]Configuration:[/cyan]")
            console.print(f"  Table Name: {table_name}")
            console.print(f"  Project Root: {project_root}")
            console.print(f"  Max Records: {max_records}")
            console.print(f"  Timeout: {timeout}s")
            console.print(f"  Output Format: {output_format}")
            console.print(f"  Validate Schema: {validate_schema}")
            console.print(f"  From Beginning: {from_beginning}")
            console.print(f"  Group ID: {group_id or 'auto-generated'}")
            console.print(f"  Verbose: {verbose}")
            
            if config:
                console.print(f"  Config File: {config}")
        
        # Calculate duration
        duration = time.time() - start_time
        
        # Display summary
        if output_format == 'pretty':
            _display_summary(0, 0, 0, duration)
        
        return 0
        
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠ Interrupted by user[/yellow]")
        return 130
    except Exception as e:
        console.print(f"\n[red]✗ Error: {str(e)}[/red]")
        if verbose:
            import traceback
            console.print("\n[red]Traceback:[/red]")
            console.print(traceback.format_exc())
        return 1


def _display_header(table_name: str, verbose: bool):
    """Display a nice header for the validation run."""
    console.print()
    console.print(Panel(
        f"[bold cyan]Validating:[/bold cyan] {table_name}\n"
        f"[dim]Topic: <to be determined>[/dim]\n"
        f"[dim]Schema Context: <to be determined>[/dim]",
        title="[bold]Kafka Record Validator[/bold]",
        border_style="cyan"
    ))
    console.print()


def _display_summary(total: int, valid: int, invalid: int, duration: float):
    """Display validation summary."""
    console.print()
    
    # Create summary table
    summary_table = RichTable(show_header=False, box=None, padding=(0, 2))
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", style="bold")
    
    summary_table.add_row("Total Records", str(total))
    
    if total > 0:
        valid_pct = (valid / total) * 100
        invalid_pct = (invalid / total) * 100
        summary_table.add_row(
            "Valid Records",
            f"{valid} ({valid_pct:.1f}%)" if valid > 0 else "0 (0%)"
        )
        summary_table.add_row(
            "Invalid Records",
            f"[red]{invalid}[/red] ({invalid_pct:.1f}%)" if invalid > 0 else "0 (0%)"
        )
    
    summary_table.add_row("Duration", f"{duration:.2f}s")
    
    console.print(Panel(
        summary_table,
        title="[bold]Summary[/bold]",
        border_style="green" if invalid == 0 else "yellow"
    ))
    console.print()


if __name__ == "__main__":
    sys.exit(main())

