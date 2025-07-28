from src.pynoodle import icrm

@icrm
class INames:
    def get_names(self) -> list[str]:
        """Return a list of names."""
        ...

    def add_name(self, name: str) -> None:
        """Add a name to the list."""
        ...

    def remove_name(self, name: str) -> None:
        """Remove a name from the list."""
        ...