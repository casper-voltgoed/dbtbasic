import abc


class BaseBackend(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create_schema(self, schema_name: str) -> None:
        pass

    @abc.abstractmethod
    def create_view(self, schema_name: str, table_name: str, query: str) -> None:
        pass

    @abc.abstractmethod
    def create_table(self, schema_name: str, table_name: str, query: str) -> None:
        pass

    @abc.abstractmethod
    def upload_csv_as_table(self, schema_name: str, table_name: str, csv_file_path: str) -> None:
        pass

    @abc.abstractmethod
    def create_index(self, schema_name: str, table_name: str, index_columns: list | str) -> None:
        pass
