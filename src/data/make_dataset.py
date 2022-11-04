# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
import pandas as pd

import os


@click.command()
@click.argument(
    "input_filepath",
    default=Path("data/raw/netflix_branded_titles_with_release_year.csv"),
    type=click.Path(exists=False),
)
@click.argument(
    "output_filepath",
    default=Path("data/interim/netflix_branded_titles_test.csv"),
    type=click.Path(),
)
def main(input_filepath, output_filepath):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info("making final data set from raw data")

    # TODO
    clean_netflix_file(input_filepath, output_filepath)
    return None


def clean_netflix_file(input_file, output_file):
    """Runs data processing scripts to turn raw data from (../raw) into
    cleaned data ready to be analyzed (saved in ../processed).
    """
    # Read in data
    df = pd.read_csv(input_file)

    # Rename title column and change year to int
    df.rename(columns={"title_desc": "combined_title"}, inplace=True)
    df["release_year"] = df["release_year"].astype("int")

    # Drop problem data
    df = manual_removals(df)

    # Extract title and season
    df["title"] = df["combined_title"].str.extract(
        r"(.+?)($|: Season \d+)", expand=False
    )[0]
    df["title"] = clean_title(df["title"])
    df["season"] = df["combined_title"].str.extract(r": Season (\d+)")

    # Manually add season to TV shows with abnormal season numbering
    manual_tv_shows = {
        "pokémon journeys the series part 2": 2,
        "pokémon master journeys the series part 1": 1,
        "pokémon master journeys the series part 3": 3,
        "pretend its a city limited series": 1,
        "rilakkumas theme park adventure: シーズン１": 1,
        "saint seiya knights of the zodiac part ii": 2,
        "tiger & bunny 2": 2,
        "the haunting of bly manor": 1,
        "tiger king the doc antle story": 1,
        "jeenyuhs a kanye trilogy": 1,
        "tiger king": 1,
        "barbarians": 1,

    }

    for i in manual_tv_shows:
        df.loc[df["title"] == i, "season"] = manual_tv_shows[i]

    # Determine TV show or movie
    df["is_tv_show"] = df["season"].notnull()

    # Output to CSV as interim data as needed
    # df.to_csv(output_file, index=False)
    return df


def add_imdb_basic(netflix_data, imdb_data='data/interim/multimatch_manual.csv', manual_data='data/interim/manual.csv'):
    # Read in data
    imdb_all = pd.read_csv(imdb_data)
    manual = pd.read_csv(manual_data)
    # Clean title
    imdb_all['title'] = clean_title(imdb_all['title'])
    # Split into movies and tv shows
    movies_only = netflix_data[(netflix_data['is_tv_show'] == False)]
    tv_shows_only = netflix_data[(netflix_data['is_tv_show'] == True)]
    # Merge movies
    movie_merge = pd.merge(movies_only, imdb_all[~imdb_all['titleType'].isin(['tvSeries', 'video', 'videoGame', 'tvPilot', 'tvMiniSeries'])], on=['title', 'release_year'], how='left', indicator=True)
    movie_good = movie_merge[movie_merge['_merge'] == 'both'].groupby('combined_title').filter(lambda x: len(x) == 1)
    # Merge tv shows
    tv_merge = pd.merge(tv_shows_only, imdb_all[imdb_all['titleType'].isin(['tvSeries', 'tvMiniSeries'])], on=['title', 'release_year'], how='left', indicator=True)
    tv_show_good = tv_show_merge[tv_show_merge['_merge'] == 'both'].groupby('combined_title').filter(lambda x: len(x) == 1)
    # Combine all into one joining dataframe
    automated_tconsts = pd.concat([movie_good[['combined_title', 'tconst']], tv_show_good[['combined_title', 'tconst']]], axis=0)
    manual_tconsts = pd.read_csv('../data/interim/manual.csv')
    tconst_joiner = pd.concat([automated_tconsts, manual_tconsts], axis=0)
    # Merge with netflix data
    netflix_data = pd.merge(netflix_data, tconst_joiner, on='combined_title', how='left')

    final_df = pd.merge(netflix_data, imdb_all, on='tconst', how='inner')
    
    return final_df 

    return netflix_data


def clean_title(title):
    # Remove punctuation
    title = title.str.replace(r"[^\w\s]", "", regex=True)
    # Remove leading and trailing whitespace
    title = title.str.strip()
    # Remove leading and trailing whitespace
    title = title.str.lower()
    return title


def manual_removals(df):
    df = df[df["combined_title"] != "TIGER & BUNNY 2"]
    df = df[df["combined_title"] != "KAKEGURUI TWIN: Season 1"]
    df = df[df["combined_title"] != "KANE CHRONICLES"]
    df = df[
        ~(
            (df["combined_title"] == "Catching Killers: Season 1")
            & (df["release_year"] == 2020)
        )
    ]
    df = df[
        ~(
            (df["combined_title"] == "The One: Season 1")
            & (df["release_date"] == 20210616)
        )
    ]
    df = df[
        ~(
            (df["combined_title"] == "KAKEGURUI TWIN: Season 1")

    return df


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
