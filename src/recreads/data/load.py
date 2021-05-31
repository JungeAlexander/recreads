import gzip
import json
from datetime import date
from pathlib import Path
from typing import Generator

import arrow
import pandas as pd


def _iterate_ratings_raw(input_file: Path) -> Generator:
    with gzip.open(input_file, "rt") as fin:
        for line in fin:
            j = json.loads(line)
            if j["rating"] > 0:
                d = arrow.get(j["date_updated"], "ddd MMM DD HH:mm:ss Z YYYY").date()
                first_day_month = date(year=d.year, month=d.month, day=1)
                yield j["user_id"], j["book_id"], first_day_month, j["rating"]


def load_ratings_raw(data_dir: Path, genre: str) -> pd.DataFrame:
    """
    Load raw ratings from an interactions file for a given genre.

    :param data_dir: directory to read interaction file from
    :param genre: genre name to be loaded
    :return: a DataFrame with columns: user id, book id, day of month, rating score
    """
    input_file = data_dir / f"goodreads_interactions_{genre}.json.gz"
    return pd.DataFrame(
        _iterate_ratings_raw(input_file),
        columns=["User_id", "Book_id", "Month", "Rating"],
    )
