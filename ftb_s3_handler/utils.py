import json
import polars as pl

def handle_nested_data(df: pl.DataFrame) -> pl.DataFrame:
    processed_df = df.clone()

    for column in df.columns:
        dtype = df[column].dtype

        if dtype in (pl.List, pl.Array):
            processed_df = processed_df.with_columns(
                pl.col(column).map_elements(
                    lambda x: json.dumps(x, default=str) if x is not None else "",
                    return_dtype=pl.Utf8
                ).alias(column)
            )

        if dtype == pl.Struct:
            processed_df = processed_df.with_columns(
                pl.col(column).map_elements(
                    lambda x: json.dumps(x, default=str) if x is not None else "",
                    return_dtype=pl.Utf8
                ).alias(column)
            )

    return processed_df