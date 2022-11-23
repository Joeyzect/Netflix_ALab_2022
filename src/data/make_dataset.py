# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd
import numpy as np

import os


def main(
    netflix_with_imdb_codes_file="data/raw/complete_manually_cleaned_data.csv",
    ratings_file="data/external/title.ratings.tsv",
    basics_file="data/external/title.basics.tsv",
    output_file="data/processed/feed_into_pipeline.csv",
):
    """
    Runs data processing scripts to turn raw data from (../raw) and (../external) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    netflix_with_imdb_codes_dir = os.path.relpath(netflix_with_imdb_codes_file)
    ratings_dir = os.path.relpath(ratings_file)
    basics_dir = os.path.relpath(basics_file)

    netflix_titles = pd.read_csv(netflix_with_imdb_codes_dir)
    ratings = pd.read_csv(ratings_dir, sep="\t")
    basics = pd.read_csv(
        basics_dir,
        sep="\t",
        usecols=["tconst", "titleType", "runtimeMinutes", "genres"],
    )

    df = netflix_titles.merge(ratings, how="left", on="tconst").merge(
        basics, how="left", on="tconst"
    )

    # Lets also combine movie with tvMovie and short with Tvshort
    df.loc[df["titleType"] == "tvMovie", "titleType"] = "movie"
    df.loc[df["titleType"] == "tvShort", "titleType"] = "short"

    # Waffles + Mochi's Holiday Feast is a holiday special of a tv show, let's label it as a tvSpecial
    df.loc[
        df["combined_title"] == "Waffles + Mochi's Holiday Feast", "titleType"
    ] = "tvSpecial"

    # My Wonderful Life Manually added data
    df.loc[df["combined_title"] == "My Wonderful Life", "averageRating"] = 5.9
    df.loc[df["combined_title"] == "My Wonderful Life", "numVotes"] = 714
    df.loc[df["combined_title"] == "My Wonderful Life", "titleType"] = "movie"
    df.loc[df["combined_title"] == "My Wonderful Life", "runtimeMinutes"] = 99
    df.loc[
        df["combined_title"] == "My Wonderful Life", "genres"
    ] = "Comedy,Drama,Romance"

    # Fix 2 tvshows with strange seasons
    df.loc[df["combined_title"] == "Monsters Inside: The 24 Faces of Billy Milligan: Limited Series", "seasons"] = 1
    df.loc[df["combined_title"] == "Room 2806: The Accusation: Limited Series", "seasons"] = 1

    # removes the storybot shorts and The New which have no data on imdb
    df = df[df["averageRating"].notnull()].copy()

    # Top 10 to int
    df["is_top10"] = df["is_top10"].astype(int)

    # Replace \N with NaN
    df.replace("\\N", np.nan, inplace=True)

    # Rename title to lower_title
    df.rename(columns={"title": "lower_title"}, inplace=True)

    # Fix dtypes
    df["runtimeMinutes"] = df["runtimeMinutes"].astype(float)
    df["numVotes"] = df["numVotes"].astype(int)

    # Turn release date into datetime
    df["release_date"] = pd.to_datetime(df["release_date"], format="%Y%m%d")

    # Add month, week, quarter
    df["release_date_quarter"] = df["release_date"].dt.quarter
    df["release_date_month"] = df["release_date"].dt.month
    df["release_date_week"] = df["release_date"].dt.isocalendar().week

    #drop duplicates
    df.drop_duplicates(subset=["combined_title"], inplace=True)

    # Output to CSV
    output_filepath = os.path.relpath(output_file)
    df.to_csv(output_filepath, index=False)

    return None


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
