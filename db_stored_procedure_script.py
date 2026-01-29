class DbStoredProcedureScript(object):

    def __init__(self, name: str, script:str):
        self.name: str = name
        self.script = script

    def __str__(self):
        return self.name
