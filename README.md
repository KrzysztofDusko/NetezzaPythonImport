# Netezza CSV Import Tool

A Python utility for importing CSV files into IBM Netezza database using named pipes and automatic data type detection.

## Features

- Automatic CSV delimiter detection (supports |, ;, \t, ,)
- Smart data type inference for columns
- Handles common date/time formats
- Supports large files through streaming
- Progress monitoring during import
- UTF-8 encoding support

## Supported Data Types

- BIGINT: For integer values
- NUMERIC: For decimal numbers
- DATE: For dates in YYYY-MM-DD format
- DATETIME: For timestamps 
- NVARCHAR: For text fields

## Usage

```bash
python main.py filename [-l LOG_DIR] [-d DRIVER]

Arguments:
  filename              Path to CSV file to import
  -l, --log_dir        Log directory path (default: C:\log)
  -d, --driver         Driver to use (default: dotnet)
```

## Example

```bash
python main.py data.csv -l C:\netezza\logs -d dotnet
```

The tool will:
1. Analyze the CSV file to determine column types
2. Generate CREATE TABLE SQL statement
3. Create a named pipe
4. Stream data through the pipe to Netezza

## Requirements

- Python 3.7+
- pywin32 package
- Windows OS (due to named pipes implementation)
- IBM Netezza client tools