class DebridError(Exception):
    def __init__(self, message: str, *, error_code: str = None, upstream_error_code: str = None):
        self.error_code = error_code
        self.upstream_error_code = upstream_error_code
        super().__init__(message)

    @property
    def status_keys(self) -> list:
        keys = []
        if self.upstream_error_code:
            keys.append(self.upstream_error_code)
        if self.error_code and self.error_code not in keys:
            keys.append(self.error_code)
        return keys
