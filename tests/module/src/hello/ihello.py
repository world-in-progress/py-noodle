from pynoodle import icrm

@icrm('test')
class IHello:
    def greet(self, index: int) -> str:
        ...