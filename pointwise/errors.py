"""Error types shared by the collector, the compute engine and the API."""

from __future__ import annotations


class PointwiseError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def as_dict(self) -> dict:
        return {"error": self.message, "code": self.code, "details": self.details}


class ValidationError(PointwiseError):
    status = 422
    code = "invalid_rules"


class NotFound(PointwiseError):
    status = 404
    code = "not_found"


class Conflict(PointwiseError):
    status = 409
    code = "conflict"


class ComputeError(PointwiseError):
    status = 500
    code = "compute_error"
