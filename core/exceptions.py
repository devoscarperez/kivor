class AppException(Exception):
    status_code = 400
    detail = "Application error"

    def __init__(self, detail: str = None):
        if detail:
            self.detail = detail


class NotFoundError(AppException):
    status_code = 404
    detail = "Resource not found"


class UnauthorizedError(AppException):
    status_code = 401
    detail = "Unauthorized"


class ForbiddenError(AppException):
    status_code = 403
    detail = "Forbidden"


class InvalidDataError(AppException):
    status_code = 400
    detail = "Invalid data"


class InternalServerError(AppException):
    status_code = 500
    detail = "Internal server error"
