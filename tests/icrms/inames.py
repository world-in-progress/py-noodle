import c_two as cc

@cc.icrm(namespace='test', version='0.0.1')
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