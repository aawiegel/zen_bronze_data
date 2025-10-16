from typing import Any
import csv


class CSVTableParser:
    """
    Parse CSV files from Databricks volumes and unpivot them to long format

    This class reads wide-format CSV files and transforms them into long format
    with each cell becoming a row containing its original position and value.
    """

    def __init__(self, config: dict[str, Any] | None = None):
        """
        Initialize the CSV parser

        Args:
            config: Optional configuration dictionary with keys:
                - header_detection_threshold: Minimum non-null columns to detect header (default: 10)
                - encoding: File encoding (default: "utf-8")
                - csv_opts: Additional options to pass to csv.reader (default: {})
        """
        self.config = config or {}

    def parse(self, file_path: str) -> list[dict[str, Any]]:
        """
        Parse a CSV file and return it in long format

        Args:
            file_path: Path to CSV file (can be Databricks volume path like /Volumes/...)

        Returns:
            List of dicts with keys: row_index, column_index, lab_provided_attribute, lab_provided_value

        Example:
            >>> parser = CSVTableParser()
            >>> records = parser.parse("/Volumes/catalog/schema/volume/vendor_a.csv")
        """
        # Load CSV file
        with open(
            file_path, "r", encoding=self.config.get("encoding", "utf-8")
        ) as file:
            reader = csv.reader(file, **self.config.get("csv_opts", {}))
            records = [[value if value else None for value in row] for row in reader]

        # Clean and process
        records = self.remove_header(
            records, min_found=self.config.get("header_detection_threshold", 10)
        )
        records = self.clean_columns(records)
        records = self.unpivot(records)

        return records

    def remove_header(
        self, records: list[list[Any]], min_found: int = 10
    ) -> list[list[Any]]:
        """
        Find the first row with enough non-null values to be considered the header

        This is useful for CSV files that might have metadata or empty rows at the top.

        Args:
            records: List of records potentially containing empty space or metadata above the data header
            min_found: Minimum number of columns that need data to be identified as the header row

        Returns:
            List of records starting from the header row
        """
        header_index = None

        for i, row in enumerate(records):
            non_nulls = sum(item is not None for item in row)
            if non_nulls >= min_found:
                header_index = i
                break

        if header_index is None:
            raise ValueError("Could not find header row.")

        return records[header_index:]

    def clean_columns(self, records: list[list[Any]]) -> list[list[Any]]:
        """
        Drop empty columns and deduplicate column names

        Args:
            records: List of records with header row as first element

        Returns:
            Records with cleaned columns
        """
        cols_to_drop = [
            index for index, column in enumerate(records[0]) if column is None
        ]
        records = [
            [item for index, item in enumerate(row) if index not in cols_to_drop]
            for row in records
        ]

        records[0] = self._dedupe_columns(records[0])

        return records

    def _dedupe_columns(self, columns: list[str]) -> list[str]:
        """
        Deduplicate column names by appending _1, _2, etc. to duplicates

        Args:
            columns: List of column names

        Returns:
            List of deduplicated column names
        """
        counts: dict[str, int] = {}
        new_columns = []

        for column in columns:
            if column not in counts:
                new_columns.append(column)
                counts[column] = 1
            else:
                new_columns.append(f"{column}_{counts[column]}")
                counts[column] += 1

        return new_columns

    def unpivot(self, records: list[list[Any]]) -> list[dict[str, Any]]:
        """
        Transform wide format to long format with position tracking

        Args:
            records: List of records with header row as first element

        Returns:
            List of dicts containing row_index, column_index, lab_provided_attribute, lab_provided_value
        """
        results = []
        attributes = records[0]
        for row_index, row in enumerate(records[1:], start=1):
            for column_index, (attribute, value) in enumerate(
                zip(attributes, row), start=1
            ):
                results.append(
                    {
                        "row_index": row_index,
                        "column_index": column_index,
                        "lab_provided_attribute": attribute,
                        "lab_provided_value": value,
                    }
                )

        return results
