class DbCmd(object):

    def __init__(self, cmd: str = None, params: str = None, params_parsed: dict = None):
        self.cmd: str = cmd
        self.params_str: str = params
        self.params_dict: dict = params_parsed
