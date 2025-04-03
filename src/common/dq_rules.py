from pyspark.sql import DataFrame


class DQRules:

    @staticmethod
    def check_nulls(df: DataFrame, columns: list):
        """Raise error if any column has nulls."""
        for col in columns:
            null_count = df.filter(f"{col} IS NULL").count()
            if null_count > 0:
                raise ValueError(f"Column '{col}' has {null_count} null values.")

    @staticmethod
    def check_unique(df: DataFrame, columns: list):
        """Raise error if column(s) are not unique."""
        total_count = df.count()
        distinct_count = df.select(columns).distinct().count()
        if total_count != distinct_count:
            raise ValueError(f"Column(s) {columns} are not unique (total={total_count}, distinct={distinct_count}).")

    @staticmethod
    def check_valid_range(df: DataFrame, column: str, min_val, max_val):
        """Raise error if any value is out of range."""
        out_of_range = df.filter((df[column] < min_val) | (df[column] > max_val)).count()
        if out_of_range > 0:
            raise ValueError(f"Column '{column}' has {out_of_range} values outside range [{min_val}, {max_val}].")

    @staticmethod
    def check_schema(df: DataFrame, expected_schema: list):
        """Check column names and order match expected schema."""
        actual_schema = df.columns
        if actual_schema != expected_schema:
            raise ValueError(f"Schema mismatch. Expected: {expected_schema}, Got: {actual_schema}")

    @staticmethod
    def check_non_empty(df: DataFrame):
        """Ensure DataFrame is not empty."""
        if df.count() == 0:
            raise ValueError("DataFrame is empty.")
