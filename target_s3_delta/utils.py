import decimal
import math
from decimal import Decimal


def float_to_decimal(value):
    """
    Walk the given data structure and turn all instances of float into double.
    """
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [float_to_decimal(child) for child in value]
    if isinstance(value, dict):
        return {k: float_to_decimal(v) for k, v in value.items()}
    return value


def numeric_schema_with_precision(schema):
    if "type" not in schema:
        return False
    if isinstance(schema["type"], list):
        if "number" not in schema["type"]:
            return False
    elif schema["type"] != "number":
        return False
    if "multipleOf" in schema:
        return True
    return "minimum" in schema or "maximum" in schema


def walk_schema_for_numeric_precision(schema):
    # Added a default max precision for cases when the schema does not specify a maximum or minimum precision
    # Set this default based on https://tools.ietf.org/html/rfc7159#section-6
    default_maximum_value = 9007199254740991
    if isinstance(schema, list):
        for v in schema:
            walk_schema_for_numeric_precision(v)
    elif isinstance(schema, dict):
        if numeric_schema_with_precision(schema):

            def get_precision(key, default=1):
                v = abs(Decimal(schema.get(key, default))).log10()
                if v < 0:
                    return round(math.floor(v))
                return round(math.ceil(v))

            scale = -1 * get_precision("multipleOf")
            digits = max(
                get_precision("minimum", default_maximum_value), get_precision("maximum", default_maximum_value)
            )
            precision = digits + scale
            if decimal.getcontext().prec < precision:
                decimal.getcontext().prec = precision
        else:
            for v in schema.values():
                walk_schema_for_numeric_precision(v)
