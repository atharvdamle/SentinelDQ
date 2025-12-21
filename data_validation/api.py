from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, Optional
from data_validation import DataValidator

app = FastAPI(title="SentinelDQ Validator")

# Initialize validator once. Persistence and metrics are enabled by env-config inside DataValidator.
_validator = DataValidator(enable_persistence=False, enable_metrics=True)


class ValidateRequest(BaseModel):
    event: Dict[str, Any]
    event_id: Optional[str] = None


@app.post("/validate")
async def validate(req: ValidateRequest):
    try:
        result = _validator.validate_event(
            req.event, event_id=req.event_id, persist=False)
        return {
            "status": result.status.value,
            "event_id": result.event_id,
            "processing_time_ms": result.processing_time_ms,
            "failures": [f.to_dict() for f in result.failures]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok"}
