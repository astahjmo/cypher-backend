from pydantic import BaseModel, Field, ConfigDict
from pydantic_core import core_schema
from typing import Any, Dict
from datetime import datetime, timezone
from bson import ObjectId


class PyObjectId(ObjectId):
    """Tipo customizado Pydantic V2 para BSON ObjectId.

    Lida com validação e serialização entre strings e ObjectId.
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: Any
    ) -> core_schema.CoreSchema:
        """Define como o Pydantic deve lidar com a validação para este tipo.

        Aceita instâncias de ObjectId ou uma representação de string válida.
        Serializa sempre para string.

        Args:
            source_type (Any): O tipo de origem sendo validado.
            handler (Any): O manipulador de esquema do Pydantic.

        Returns:
            core_schema.CoreSchema: O esquema CoreSchema para validação e serialização.
        """
        object_id_schema = core_schema.union_schema(
            [
                core_schema.is_instance_schema(ObjectId),
                core_schema.no_info_plain_validator_function(cls.validate_object_id),
            ],
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: str(v)
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
        """Função validadora para Pydantic.

        Verifica se o valor é uma instância de ObjectId ou uma string válida
        que pode ser convertida para ObjectId.

        Args:
            v (Any): O valor a ser validado.

        Returns:
            ObjectId: O ObjectId validado.

        Raises:
            ValueError: Se o valor não for um ObjectId ou uma string válida.
        """
        if isinstance(v, ObjectId):
            return v
        if isinstance(v, str) and ObjectId.is_valid(v):
            return ObjectId(v)
        raise ValueError("Invalid ObjectId")

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: core_schema.CoreSchema, handler: Any
    ) -> Dict[str, Any]:
        """Define como este tipo deve ser representado no JSON Schema (OpenAPI).

        Especifica que o tipo é 'string' com formato 'objectid'.

        Args:
            core_schema (core_schema.CoreSchema): O esquema core Pydantic.
            handler (Any): O manipulador de esquema JSON do Pydantic.

        Returns:
            Dict[str, Any]: O esquema JSON atualizado.
        """
        json_schema = handler(core_schema)
        json_schema.update(type="string", format="objectid")
        return json_schema


def now_utc():
    """Retorna a data e hora atual com fuso horário UTC.

    Returns:
        datetime: Objeto datetime com timezone UTC.
    """
    return datetime.now(timezone.utc)


def serialize_datetime(dt: datetime) -> str:
    """Garante que o datetime esteja em UTC e o formata para ISO 8601 com 'Z'.

    Args:
        dt (datetime): O objeto datetime a ser serializado.

    Returns:
        str: A string formatada em ISO 8601 (ex: '2023-10-27T10:30:00.123Z').
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    dt_str = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return dt_str[:-3] + "Z"


class BaseModelWithDates(BaseModel):
    """Modelo base Pydantic que fornece configuração comum, incluindo serialização de datas.

    Configura `populate_by_name`, `arbitrary_types_allowed` e um codificador JSON
    personalizado para objetos `datetime` usando `serialize_datetime`.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={datetime: serialize_datetime},
    )
