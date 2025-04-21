from pydantic import BaseModel, Field, ConfigDict
from pydantic_core import core_schema
from typing import Any, Dict
from datetime import datetime, timezone
from bson import ObjectId

# --- Custom PyObjectId Class for Pydantic V2 ---
class PyObjectId(ObjectId):
    """
    Custom Pydantic V2 type for BSON ObjectId.
    Handles validation and serialization.
    """
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: Any
    ) -> core_schema.CoreSchema:
        """
        Defines how Pydantic should handle validation for this type.
        Accepts ObjectId or a valid string representation.
        """
        object_id_schema = core_schema.union_schema(
            [
                core_schema.is_instance_schema(ObjectId),
                core_schema.no_info_plain_validator_function(cls.validate_object_id),
            ],
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: str(v) # Always serialize to string
            ),
        )
        return core_schema.json_or_python_schema(
            json_schema=object_id_schema,
            python_schema=core_schema.union_schema(
                [core_schema.is_instance_schema(cls), object_id_schema]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: str(v)
            ),
        )

    @classmethod
    def validate_object_id(cls, v: Any) -> ObjectId:
        """Validator function for Pydantic."""
        if isinstance(v, ObjectId):
            return v
        if isinstance(v, str) and ObjectId.is_valid(v):
            return ObjectId(v)
        raise ValueError("Invalid ObjectId")

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: core_schema.CoreSchema, handler: Any
    ) -> Dict[str, Any]:
        """
        Defines how this type should be represented in JSON Schema (OpenAPI).
        """
        json_schema = handler(core_schema)
        json_schema.update(type='string', format='objectid')
        return json_schema

# Helper function for timezone-aware UTC default factory
def now_utc():
    return datetime.now(timezone.utc)

# Helper function for datetime serialization
def serialize_datetime(dt: datetime) -> str:
    """Ensure datetime is UTC and format to ISO 8601 with Z."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    dt_str = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
    return dt_str[:-3] + 'Z'

# --- Base Model with Date Serialization Config ---
class BaseModelWithDates(BaseModel):
    """Base model providing common configuration including date serialization."""
    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={
            datetime: serialize_datetime
        }
    )
