from pydantic import BaseModel
from datetime import timedelta

class ConnectorConfig(BaseModel):
    max_delta_time: timedelta