class MissingParticipantRowException(Exception):
    def __init__(self, base_exception:Exception):
        self.base_exception = base_exception
        super().__init__()