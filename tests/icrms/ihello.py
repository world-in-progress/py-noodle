from pynoodle import icrm

@icrm('test', '0.0.1')
class IHello:
    def greet(self, index: int) -> str:
        ...