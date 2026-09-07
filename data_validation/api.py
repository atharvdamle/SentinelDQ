import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, Optional
from data_validation import DataValidator

app = FastAPI(title="SentinelDQ Validator")

# Persisting the verdict costs one pooled upsert per request. The consumer
# calls this endpoint synchronously with a short timeout (VALIDATOR_TIMEOUT,
# default 0.5s) and drops events fail-closed when it expires, so set
# VALIDATOR_PERSIST=false to take the write off the hot path if that ever
# becomes the binding constraint.
_persist = os.getenv("VALIDATOR_PERSIST", "true").lower() not in ("false", "0", "no")
_validator = DataValidator(enable_persistence=_persist, enable_metrics=True)


class ValidateRequest(BaseModel):
    event: Dict[str, Any]
    event_id: Optional[str] = None


@app.post("/validate")
async def validate(req: ValidateRequest):
    try:
        result = _validator.validate_event(req.event, event_id=req.event_id, persist=_persist)
        return {
            "status": result.status.value,
            "event_id": result.event_id,
            "processing_time_ms": result.processing_time_ms,
            "failures": [f.to_dict() for f in result.failures],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok"}
