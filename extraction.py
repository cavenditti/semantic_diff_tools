import difflib
from io import StringIO
import subprocess

import polars as pl


def filter_entries(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(~pl.col("text").str.starts_with("###"))


def extract_text(filename: str) -> pl.DataFrame:
    # 1. Run pdftotext with -tsv, writing to stdout ("-")
    proc = subprocess.run(
        # ['pdftotext', '-q', '-nopgbrk', '-tsv', 'somefile.pdf', '-'],
        ["pdftotext", "-q", "-nopgbrk", "-tsv", filename, "-"],
        capture_output=True,
        text=True,
        check=True,
    )

    # load tsv file in a polars DataFrame
    return pl.read_csv(
        StringIO(proc.stdout),
        has_header=True,
        truncate_ragged_lines=True,
        separator="\t",
    ).with_row_index()


def extract(file1_path, file2_path) -> list:
    # Extract text and load into a Polars DataFrame
    df1 = extract_text(file1_path)
    df2 = extract_text(file2_path)

    # Ignore non-visible text
    # TODO improve this
    df1_f = filter_entries(df1)
    df2_f = filter_entries(df2)

    # These are used to later retrieve the exact matching row in the original dataframes
    text1_references = df1_f.select("index", "text")
    text2_references = df2_f.select("index", "text")

    text1 = text1_references["text"].to_list()
    text2 = text2_references["text"].to_list()

    s = difflib.SequenceMatcher(None, text1, text2)
    matches = s.get_matching_blocks()
    return [
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


if __name__ == "__main__":
    extract(
        file1_path="SUBSET-026-1 v360.pdf",
        file2_path="SUBSET-026-1 v400.pdf",
    )
