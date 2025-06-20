from pathlib import Path
from sql_parser import UniversalSQLParser

def calculate_total_usage(
    sql_filename: str = "meter_readings_data.sql",
    table_name: str = "fact_meter_readings",
    value_column: str = "col_4"
):
    """
    Calculate total usage from SQL data file.
    
    Args:
        sql_filename: Name of the SQL file in the sql directory
        table_name: Name of the table containing the data
        value_column: Column name containing the usage values
    """
    # Get the parent directory path
    root_dir = Path(__file__).parent.parent
    sql_dir = root_dir / "sql"

    # Create the parser instance
    parser = UniversalSQLParser(sql_dir)

    # Initialize total
    total = 0.0
    record_count = 0

    try:
        # Stream records (pass just the filename, not full path)
        for record in parser.stream_records(sql_filename):
            if record["table"] == table_name:
                value = record["data"].get(value_column)
                if value is not None:
                    try:
                        total += float(value)
                        record_count += 1
                    except (ValueError, TypeError):
                        print(f"Warning: Could not convert value {value} to float")
        
        print(f"Total kWh usage: {total:.2f}")
        print(f"Calculated from {record_count} records")
        
        return total
    
    except FileNotFoundError:
        print(f"Error: SQL file '{sql_filename}' not found in {sql_dir}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

if __name__ == "__main__":
    # Example usage with default parameters
    calculate_total_usage()
    
    # Example with custom parameters
    # calculate_total_usage(
    #     sql_filename="another_meter_data.sql",
    #     table_name="readings",
    #     value_column="col_2"
    # )