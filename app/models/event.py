from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import uuid
from datetime import datetime

class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    user_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    payload: Dict[str, Any] = Field(default_factory=dict)
