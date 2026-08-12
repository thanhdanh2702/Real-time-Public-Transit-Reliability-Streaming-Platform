import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError, ValidationError

SchemaKey = tuple[str, int]

SCHEMA_FILES: dict[SchemaKey, str] = {
    ("vehicle_position", 1): "vehicle_position_v1.json",
    ("trip_update", 1): "trip_update_v1.json",
    ("service_alert", 1): "service_alert_v1.json",
}


class SchemaLoadError(RuntimeError):
    """Khong the tai hoac khoi tao JSON Schema."""


class EventValidationError(ValueError):
    """Event khong tuan theo JSON Schema"""


class SchemaValidator:
    def __init__(self, contract_directory: Path) -> None:
        self._validators: dict[SchemaKey, Draft202012Validator] = {}
        format_checker = FormatChecker()

        for schema_key, filename in SCHEMA_FILES.items():
            schema_path = contract_directory / filename

            try:
                with schema_path.open(encoding="utf-8") as file:
                    schema = json.load(file)

                Draft202012Validator.check_schema(schema)

            except (OSError, json.JSONDecodeError, SchemaError) as exc:
                raise SchemaLoadError(f"Could not load schema: {schema_path}") from exc

            self._validators[schema_key] = Draft202012Validator(
                schema, format_checker=format_checker
            )

    def validate(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type")
        schema_version = event.get("schema_version")

        if not isinstance(event_type, str):
            raise EventValidationError("event_type must be a string")

        if type(schema_version) is not int:
            raise EventValidationError("schema_version must be an integer")

        validator = self._validators.get((event_type, schema_version))

        if validator is None:
            raise EventValidationError(f"Unsupported event schema: {event_type} v{schema_version}")

        try:
            validator.validate(event)

        except ValidationError as exc:
            field_path = ".".join(str(part) for part in exc.absolute_path)
            field_path = field_path or "<root>"

            raise EventValidationError(
                f"{event_type} v{schema_version} at {field_path}: {exc.message}"
            ) from exc
