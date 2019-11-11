class Struct:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FlowDefinition(Struct):
    def __init__(self, **kwargs):
        super(FlowDefinition, self).__init__(kwargs)


class Flow(Struct):
    def __init__(self, **kwargs):
        super(Flow, self).__init__(kwargs)
