class RequiredParameterNotSetError(Exception):
    def __init__(self, parameter_name):
        self.msg = f"A required parameter is missing: {parameter_name}"
        super().__init__(self.msg)