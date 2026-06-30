from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

from t2c_data.core.config import settings


class Base(DeclarativeBase):
    metadata = MetaData(schema=settings.db_schema)
