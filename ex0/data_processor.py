from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """Common processing interface shared by every data processor."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.total = 0
        self._items: list[tuple[int, str]] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        """Pop the oldest stored entry with its processing order."""
        if not self._items:
            raise IndexError(f"{self.name} has no data to output")
        return self._items.pop(0)

    def _store(self, value: str) -> None:
        self._items.append((self.total, value))
        self.total += 1


class NumericProcessor(DataProcessor):

    def __init__(self) -> None:
        super().__init__("Numeric Processor")

    def _is_number(self, data: Any) -> bool:
        return isinstance(data, (int, float)) and not isinstance(data, bool)

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return bool(data) and all(self._is_number(i) for i in data)
        return self._is_number(data)

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        values = data if isinstance(data, list) else [data]
        for value in values:
            self._store(str(value))


class TextProcessor(DataProcessor):

    def __init__(self) -> None:
        super().__init__("Text Processor")

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return bool(data) and all(isinstance(i, str) for i in data)
        return isinstance(data, str)

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        values = data if isinstance(data, list) else [data]
        for value in values:
            self._store(value)


class LogProcessor(DataProcessor):

    def __init__(self) -> None:
        super().__init__("Log Processor")

    def _is_entry(self, data: Any) -> bool:
        return (
            isinstance(data, dict)
            and bool(data)
            and all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in data.items()
            )
        )

    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return bool(data) and all(self._is_entry(i) for i in data)
        return self._is_entry(data)

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            self._store(": ".join(entry.values()))


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    numeric = NumericProcessor()
    print("\nTesting Numeric Processor...")
    print(f" Trying to validate input '42': {numeric.validate(42)}")
    print(f" Trying to validate input 'Hello': {numeric.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior "
          "validation:")
    try:
        numeric.ingest("foo")
    except ValueError as error:
        print(f" Got exception: {error}")
    numbers: list[int | float] = [1, 2, 3, 4, 5]
    print(f" Processing data: {numbers}")
    numeric.ingest(numbers)
    print(" Extracting 3 values...")
    for _ in range(3):
        order, value = numeric.output()
        print(f" Numeric value {order}: {value}")

    text = TextProcessor()
    print("\nTesting Text Processor...")
    print(f" Trying to validate input '42': {text.validate(42)}")
    words = ["Hello", "Nexus", "World"]
    print(f" Processing data: {words}")
    text.ingest(words)
    print(" Extracting 1 value...")
    order, value = text.output()
    print(f" Text value {order}: {value}")

    log = LogProcessor()
    print("\nTesting Log Processor...")
    print(f" Trying to validate input 'Hello': {log.validate('Hello')}")
    entries = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(f" Processing data: {entries}")
    log.ingest(entries)
    print(" Extracting 2 values...")
    for _ in range(2):
        order, value = log.output()
        print(f" Log entry {order}: {value}")


if __name__ == "__main__":
    main()