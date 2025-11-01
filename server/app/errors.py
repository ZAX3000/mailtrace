# app/errors.py
from __future__ import annotations
from flask import jsonify

class APIError(Exception):
    status_code = 400
    error = "APIError"
    def __init__(self, message: str = "", *, details: dict | None = None, status_code: int | None = None):
        super().__init__(message)
        if status_code is not None:
            self.status_code = status_code
        self.message = message or self.error
        self.details = details

    def to_response(self):
        payload = {"error": {"type": self.__class__.__name__, "message": self.message}}
        if self.details:
            payload["error"]["details"] = self.details
        return jsonify(payload), self.status_code

class BadRequest(APIError):
    status_code = 400
    error = "BadRequest"

class Unauthorized(APIError):
    status_code = 401
    error = "Unauthorized"

class NotFound(APIError):
    status_code = 404
    error = "NotFound"

class ValidationError(APIError):
    status_code = 422
    error = "ValidationError"

class KnownUserError(APIError):
    status_code = 400
    error = "KnownUserError"

class Conflict(APIError):
    status_code = 409
    error = "Conflict"


def register_error_handlers(app):
    # Catch our APIError family
    @app.errorhandler(APIError)
    def _api_error(err: APIError):
        return err.to_response()

    # Optional: coerce generic exceptions to JSON in debug-off mode
    # from werkzeug.exceptions import HTTPException
    # @app.errorhandler(HTTPException)
    # def _http_error(e: HTTPException):
        # return jsonify({"error": {"type": e.__class__.__name__, "message": e.description}}), e.code