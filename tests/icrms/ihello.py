from src.pynoodle import icrm

@icrm
class IHello:
    def set_name(self, name: str) -> None:
        ...
    
    def greet(self, index: int) -> str:
        ...