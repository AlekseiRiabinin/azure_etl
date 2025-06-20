import re
from pathlib import Path
from typing import Dict, List, Generator, Union, Any
import json

class UniversalSQLParser:
    """
    Schema-agnostic SQL parser that handles any INSERT INTO statements.
    Converts all values to proper Python types (int, float, bool, None, str).
    """
    
    def __init__(self, sql_dir: Path):
        self.sql_dir = sql_dir
    
    def parse_sql_file(self, filename: str) -> Dict[str, List[Dict[str, Any]]]:
        """Parse a single SQL file into {table_name: [record1, record2]}"""
        filepath = self.sql_dir / filename
        content = filepath.read_text(encoding='utf-8')
        
        # Improved comment/TRUNCATE removal
        content = re.sub(r'--.*?$|/\*.*?\*/', '', content, flags=re.MULTILINE|re.DOTALL)
        content = re.sub(r'TRUNCATE TABLE \w+;[\n\r]*', '', content, flags=re.IGNORECASE)
        
        # Better INSERT pattern that handles:
        # 1. Optional column names
        # 2. Multi-line VALUES
        # 3. Multiple value groups
        inserts = re.finditer(
            r"INSERT\s+INTO\s+(\w+)\s*(?:\([^)]*\))?\s*VALUES\s*((?:\([^)]+\)(?:,\s*)?)+)\s*;",
            content,
            re.IGNORECASE|re.DOTALL
        )
        
        result = {}
        for insert in inserts:
            table = insert.group(1)
            all_values = insert.group(2)
            
            # Find all individual value groups between parentheses
            value_groups = re.finditer(r"\(([^)]+)\)", all_values)
            
            records = []
            for values in value_groups:
                # Improved value splitting that handles commas inside quotes
                raw_values = self._split_sql_values(values.group(1))
                records.append({
                    f"col_{j}": self._convert_value(val)
                    for j, val in enumerate(raw_values)
                })
            
            result[table] = records
        
        return result

    def _convert_value(self, value: str) -> Any:
        """Convert raw SQL string value to proper Python type"""
        value = value.strip()
        if not value:  # Handle empty strings
            return None
        if value.upper() == 'NULL':
            return None
        elif value.upper() == 'TRUE':
            return True
        elif value.upper() == 'FALSE':
            return False
        try:
            # Try to convert to int first
            return int(value)
        except ValueError:
            try:
                # Then try float
                return float(value)
            except ValueError:
                # Remove surrounding quotes if they exist
                if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
                    return value[1:-1]
                return value

    def _split_sql_values(self, value_string: str) -> List[str]:
        """Safely split SQL values considering quoted commas"""
        parts = []
        current = []
        in_quote = False
        quote_char = None
        
        for char in value_string.strip():
            if char in ('"', "'") and (not in_quote or char == quote_char):
                if in_quote:
                    in_quote = False
                    quote_char = None
                else:
                    in_quote = True
                    quote_char = char
                current.append(char)
            elif char == ',' and not in_quote:
                parts.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        
        if current:
            parts.append(''.join(current).strip())
        
        return parts
    
    def stream_records(self, filename: str) -> Generator[Dict[str, Union[str, Dict]], None, None]:
        """Stream records one by one from a specific SQL file with file/table context"""
        data = self.parse_sql_file(filename)
        for table, records in data.items():
            for record in records:
                yield {
                    "source_file": filename,
                    "table": table,
                    "data": record
                }

# Example Usage
if __name__ == "__main__":
    sql_dir = Path(__file__).parent.parent / "sql"
    parser = UniversalSQLParser(sql_dir)
    
    for i, record in enumerate(parser.stream_records("meter_readings_data.sql")):
        print(f"Record {i}: {record['table']} from {record['source_file']}")
        print(f"Data: {record['data']}")
        if i >= 5:  # Show first 5 records
            break