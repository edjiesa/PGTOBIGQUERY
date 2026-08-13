import re
import json
import datetime
import decimal
from typing import Dict, Any, Tuple
import pyarrow as pa
from google.cloud import bigquery


def sanitize_bq_column_name(name: str) -> str:
    """
    Sanitizes PostgreSQL column names to strictly comply with BigQuery naming rules:
    - Must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_)
    - Cannot start with a digit
    - Max 300 characters
    """
    if not name:
        return "unnamed_column"
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', str(name).strip())
    if clean and clean[0].isdigit():
        clean = f"col_{clean}"
    return clean[:300] or "unnamed_column"



def sanitize_bq_table_id(table_name: str) -> str:
    """
    Sanitizes PostgreSQL table names to comply with BigQuery table ID naming rules:
    - Must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_)
    - Cannot start with a digit
    - Max 1024 characters
    """
    if not table_name:
        return "unnamed_table"
    clean = re.sub(r'[^a-zA-Z0-9_]', '_', str(table_name).strip())
    if clean and clean[0].isdigit():
        clean = f"tbl_{clean}"
    return clean[:1024] or "unnamed_table"


def postgres_to_bigquery_type(pg_type: str) -> Tuple[str, str]:

    """
    Maps PostgreSQL 10.4 data type string to (BigQuery Type, Mode).
    Returns (bq_type, mode) e.g., ("INT64", "NULLABLE") or ("STRING", "REPEATED").
    """
    clean_type = pg_type.lower().strip()
    is_array = False

    # Handle Postgres array notation e.g., "integer[]", "_int4"
    if clean_type.endswith("[]") or clean_type.startswith("_"):
        is_array = True
        if clean_type.endswith("[]"):
            clean_type = clean_type[:-2]
        elif clean_type.startswith("_"):
            clean_type = clean_type[1:]

    # Remove parameters like varchar(255) or numeric(10, 2)
    if "(" in clean_type:
        clean_type = clean_type.split("(")[0].strip()

    # Map base type
    if clean_type in ("smallint", "integer", "bigint", "smallserial", "serial", "bigserial", "oid", "int2", "int4", "int8"):
        bq_type = "INT64"
    elif clean_type in ("real", "double precision", "float4", "float8"):
        bq_type = "FLOAT64"
    elif clean_type in ("numeric", "decimal"):
        bq_type = "NUMERIC"
    elif clean_type in ("boolean", "bool"):
        bq_type = "BOOL"
    elif clean_type in ("date",):
        bq_type = "DATE"
    elif clean_type in ("timestamp without time zone", "timestamp"):
        bq_type = "DATETIME"
    elif clean_type in ("timestamp with time zone", "timestamptz"):
        bq_type = "TIMESTAMP"
    elif clean_type in ("time", "time without time zone", "time with time zone", "timetz"):
        bq_type = "TIME"
    elif clean_type in ("json", "jsonb"):
        bq_type = "JSON"
    elif clean_type in ("bytea",):
        bq_type = "BYTES"
    else:
        # Default fallback for text, varchar, char, uuid, inet, xml, interval, enums, etc.
        bq_type = "STRING"

    mode = "REPEATED" if is_array else "NULLABLE"
    return bq_type, mode


def postgres_to_pyarrow_field(col_name: str, pg_type: str, is_nullable: bool = True) -> pa.Field:
    """
    Constructs a PyArrow Field from a PostgreSQL column definition.
    """
    bq_type, mode = postgres_to_bigquery_type(pg_type)
    # Always allow NULLs in PyArrow to prevent crashes when dirty data is sanitized to None
    nullable = True

    if mode == "REPEATED":
        # Base element type inside list
        if bq_type == "INT64":
            elem_type = pa.int64()
        elif bq_type == "FLOAT64":
            elem_type = pa.float64()
        elif bq_type == "BOOL":
            elem_type = pa.bool_()
        else:
            elem_type = pa.string()
        pa_type = pa.list_(elem_type)
    else:
        if bq_type == "INT64":
            pa_type = pa.int64()
        elif bq_type == "FLOAT64":
            pa_type = pa.float64()
        elif bq_type == "NUMERIC":
            pa_type = pa.decimal128(38, 9)
        elif bq_type == "BOOL":
            pa_type = pa.bool_()
        elif bq_type == "DATE":
            pa_type = pa.date32()
        elif bq_type == "DATETIME":
            pa_type = pa.timestamp('us')
        elif bq_type == "TIMESTAMP":
            pa_type = pa.timestamp('us', tz='UTC')
        elif bq_type == "TIME":
            pa_type = pa.time64('us')
        elif bq_type == "BYTES":
            pa_type = pa.binary()
        elif bq_type == "JSON":
            pa_type = pa.string()  # Parquet string format for JSON loading
        else:
            pa_type = pa.string()

    return pa.field(col_name, pa_type, nullable=nullable)


def convert_value_for_pyarrow(val: Any, bq_type: str, mode: str) -> Any:
    """
    Sanitizes raw PostgreSQL cell values into PyArrow / Parquet compatible representations.
    Safely handles timestamps, dates, numerics, bytea, json, and exotic types without crashing PyArrow.
    """
    if val is None:
        return None

    try:
        if mode == "REPEATED":
            if isinstance(val, (list, tuple)):
                return [str(item) if not isinstance(item, (int, float, bool)) else item for item in val]
            return [str(val)]

        if bq_type == "JSON":
            if isinstance(val, (dict, list)):
                return json.dumps(val, default=str)
            return str(val)

        if bq_type in ("DATETIME", "TIMESTAMP"):
            if isinstance(val, datetime.datetime):
                if val.year < 1900 or val.year > 2200:
                    return None
                return val
            if isinstance(val, datetime.date):
                if val.year < 1900 or val.year > 2200:
                    return None
                return datetime.datetime.combine(val, datetime.time.min)
            val_str = str(val).strip().lower()
            if not val_str or val_str.startswith("0000") or "infinity" in val_str or "bc" in val_str:
                return None
            try:
                dt = datetime.datetime.fromisoformat(val_str.replace("z", "+00:00"))
                if dt.year < 1900 or dt.year > 2200:
                    return None
                return dt
            except Exception:
                return None

        if bq_type == "DATE":
            if isinstance(val, datetime.date):
                if val.year < 1900 or val.year > 2200:
                    return None
                return val
            if isinstance(val, datetime.datetime):
                if val.year < 1900 or val.year > 2200:
                    return None
                return val.date()
            val_str = str(val).strip().lower()
            if not val_str or val_str.startswith("0000") or "infinity" in val_str or "bc" in val_str:
                return None
            try:
                d = datetime.date.fromisoformat(val_str)
                if d.year < 1900 or d.year > 2200:
                    return None
                return d
            except Exception:
                return None

        if bq_type == "TIME":
            if isinstance(val, datetime.time):
                return val
            if isinstance(val, datetime.datetime):
                return val.time()
            val_str = str(val).strip().lower()
            if not val_str or "infinity" in val_str or "bc" in val_str:
                return None
            try:
                # Python 3.7+ supports fromisoformat for time as well, but let's be safe
                # Time string looks like '14:36:34' or '14:36:34.123456'
                return datetime.time.fromisoformat(val_str)
            except Exception:
                return None

        if bq_type == "NUMERIC" or bq_type == "FLOAT64":
            if isinstance(val, (int, float, decimal.Decimal)):
                val_str_lower = str(val).lower()
                if "nan" in val_str_lower or "inf" in val_str_lower:
                    return None
                if bq_type == "NUMERIC" and not isinstance(val, decimal.Decimal):
                    try:
                        d = decimal.Decimal(str(val))
                        return d.quantize(decimal.Decimal('0.000000001'), rounding=decimal.ROUND_HALF_UP)
                    except decimal.InvalidOperation:
                        return None
                if bq_type == "NUMERIC" and isinstance(val, decimal.Decimal):
                    try:
                        return val.quantize(decimal.Decimal('0.000000001'), rounding=decimal.ROUND_HALF_UP)
                    except decimal.InvalidOperation:
                        return None
                return float(val) if bq_type == "FLOAT64" and isinstance(val, decimal.Decimal) else val

            val_str = str(val).replace("$", "").replace(",", "").strip().lower()
            if not val_str or "nan" in val_str or "inf" in val_str:
                return None
            try:
                if bq_type == "FLOAT64":
                    return float(val_str)
                else:
                    d = decimal.Decimal(val_str)
                    # Quantize to 9 decimal places to match PyArrow pa.decimal128(38, 9) schema
                    # and prevent 'Rescaling Decimal value would cause data loss' Arrow exception
                    return d.quantize(decimal.Decimal('0.000000001'), rounding=decimal.ROUND_HALF_UP)
            except Exception:
                return None

        if bq_type == "INT64":
            if isinstance(val, int):
                return val
            val_str = str(val).strip().lower()
            if not val_str or "nan" in val_str or "inf" in val_str:
                return None
            try:
                return int(float(val_str))
            except Exception:
                return None


        if bq_type == "BOOL":
            if isinstance(val, bool):
                return val
            return str(val).lower() in ("true", "1", "t", "yes", "y")

        if bq_type == "BYTES":
            if isinstance(val, memoryview):
                return bytes(val)
            if isinstance(val, bytes):
                return val
            return str(val).encode('utf-8')

        return str(val)
    except Exception:
        return str(val) if bq_type == "STRING" else None

