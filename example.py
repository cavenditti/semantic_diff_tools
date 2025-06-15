# /// script
# requires-python = ">=3.13"
# dependencies = ["polars"]
# ///


import polars as pl
import difflib


def filter_entries(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(~pl.col("text").str.starts_with("###"))


def main() -> None:
    file1_path = "SUBSET-026-1 v360.txt"
    file2_path = "SUBSET-026-1 v400.txt"

    try:
        # Read files as plain text using Polars
        # Polars can read CSV/text files, treating each line as a record.
        # We'll read it as a single column of text.
        df1 = pl.read_csv(
            file1_path, has_header=True, truncate_ragged_lines=True, separator="\t"
        ).with_row_index()
        df2 = pl.read_csv(
            file2_path, has_header=True, truncate_ragged_lines=True, separator="\t"
        ).with_row_index()

        df1_f = filter_entries(df1)
        df2_f = filter_entries(df2)

        # These are used to later retrieve the exact matching row in the original dataframes
        text1_references = df1_f.select("index", "text")
        text2_references = df2_f.select("index", "text")

        text1 = text1_references["text"].to_list()
        text2 = text2_references["text"].to_list()

        print(text1_references.shape)
        print(text2_references.shape)

        s = difflib.SequenceMatcher(None, text1, text2)
        matches = s.get_matching_blocks()
        diff_list = [
            [
                df1.filter(
                    (pl.col("index") >= text1_references[m.a, "index"])
                    & (pl.col("index") <= text1_references[m.a + m.size, "index"])
                )
                if m.a < len(text1)
                else None,
                df2.filter(
                    (pl.col("index") >= text2_references[m.b, "index"])
                    & (pl.col("index") <= text2_references[m.b + m.size, "index"])
                )
                if m.b < len(text2)
                else None,
            ]
            for m in matches
        ]

        breakpoint()

    except FileNotFoundError:
        print(
            "Error: One or both files not found. Please ensure 'SUBSET-026-1 v360.txt' and 'SUBSET-026-1 v400.txt' exist in the same directory."
        )
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
