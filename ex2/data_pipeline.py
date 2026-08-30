from abc import ABC, abstractmethod
from typing import Any, Protocol


class InvalidData(Exception):
    """Raised when a processor receives data it cannot ingest."""


class EmptyProcessor(Exception):
    """Raised when output is called on a processor holding no data."""


class DataProcessor(ABC):

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

    def __str__(self) -> str:
        return (f"{self.name}: total {self.total} items processed, "
                f"remaining {self.remaining} on processor")

    def output(self) -> tuple[int, str]:
        if not self._items:
            raise EmptyProcessor(f"{self.name} has no data to output")
        return self._items.pop(0)

    @property
    def remaining(self) -> int:
        return len(self._items)

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
            raise InvalidData("Improper numeric data")
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
            raise InvalidData("Improper text data")
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
            raise InvalidData("Improper log data")
        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            self._store(": ".join(entry.values()))


class ExportPlugin(Protocol):

    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class DataStream:

    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            for proc in self._processors:
                if proc.validate(element):
                    proc.ingest(element)
                    break
            else:
                print("DataStream error - Can't process element in "
                      f"stream: {element}")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            data = [proc.output() for _ in range(min(nb, proc.remaining))]
            if data:
                plugin.process_output(data)

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            print(proc)


class CsvExport:

    def _escape(self, value: str) -> str:
        if any(char in value for char in ',"\n'):
            return '"' + value.replace('"', '""') + '"'
        return value

    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(self._escape(value) for _, value in data))


class JsonExport:

    def _escape(self, value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        fields = ", ".join(
            f'"item_{order}": "{self._escape(value)}"'
            for order, value in data
        )
        print("{" + fields + "}")


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")

    print("\nInitialize Data Stream...")
    stream = DataStream()
    print()
    stream.print_processors_stats()

    print("\nRegistering Processors")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    batch: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING",
             "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print(f"\nSend first batch of data on stream: {batch}")
    stream.process_stream(batch)
    print()
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CsvExport())
    print()
    stream.print_processors_stats()

    second: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {"log_level": "NOTICE",
             "log_message": "Certificate expires in 10 days"},
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print(f"\nSend another batch of data: {second}")
    stream.process_stream(second)
    print()
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JsonExport())
    print()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
