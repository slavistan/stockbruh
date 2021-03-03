"""RSS utility."""
import logging
import sqlite3
from pathlib import Path

import bs4

import pandas as pd

import requests

DEFAULT_RSS_FIELD_NAMES = {"link": "link", "guid": "guid", "pubDate": "pubDate",
                           "title": "title", "description": "description"}
DEFAULT_RSS_DBTABLE_KEYS = ["guid", "link"]


def feeds_to_dataframe(urls: list, tags: dict = DEFAULT_RSS_FIELD_NAMES) -> pd.DataFrame:
    """Download RSS feeds and return as dataframe.

    Non-existing tags or tags without content are stored as empty strings "".

    Parameters
    ----------
    urls: list of str
        URL strings, e.g. https://news.co.uk/rss.xml

    tags: dict (optional)
        Key-value pairs of RSS tags to fetch and their corresponding colum
        name in the output table. By default, the tags *link*, *guid*,
        *pubDate*, *title* and *description* are downloaded and their
        information stored in eponymous columns. Keys and values must be
        unique.

    Returns
    -------
    pandas.DataFrame
        All records retrieved for the specified urls. Columns names are
        determined by the values of 'tags'.

    Examples
    -----------
    To limit the RSS tags to *link* and *pubDate* and to name the corresponding
    dataframe columns *rss_link* and *rss_pubdate* call

        urls = ["https://www.finanznachrichten.de/rss-nachrichten-meistgelesen",
                "https://www.finanznachrichten.de/rss-marktberichte"]
        feeds_to_dataframe(urls, tags={"link": "rss_link", "pubDate": "rss_pubdate"})
    """
    df = pd.DataFrame(columns=[val for val in tags.values()], dtype="string")
    for url in urls:
        response = requests.get(url)
        if response.status_code == 200:
            soup = bs4.BeautifulSoup(response.text, 'xml').find("rss")
            if soup is not None:
                for item in soup.find_all("item"):
                    record = {}
                    for rss_tag, field_name in tags.items():
                        tag = item.find(rss_tag)
                        record[field_name] = tag.text if tag is not None else ""
                    df = df.append(record, ignore_index=True)
        else:
            logging.error(f"Download of RSS feed {url} failed.")
    return df


def feeds_to_database(urls: list, dbpath: str, tablename: str = "items", tags: dict = DEFAULT_RSS_FIELD_NAMES,
                      keys: list = DEFAULT_RSS_DBTABLE_KEYS) -> None:
    """Download RSS feeds and store to sqlite database.

    Parameters
    ----------
    urls: list of str
        URL strings, e.g. https://news.co.uk/rss.xml

    dbpath: str
        Relative or absolute path to sqlite database. Database will be created if it does not exist.

    tablename: str (optional)
        Name of database table where information will be stored. Default:
        "items".

    tags: dict (optional)
        Key-value pairs of RSS tags to fetch and their corresponding colum
        name in the database table. By default, the tags *link*, *guid*,
        *pubDate*, *title* and *description* are downloaded and their
        information stored in eponymous columns. Keys and values must be
        unique.

    keys: list of str (optional)
        Subset of column names in *tags*'s values which shall constitue the
        compound primary key. Must be specified if non-default *tags* are used.
        By default, the *link* and *guid* column are used as primary key.

    Examples
    -----------
    The following call will fetch the RSS tags *link*, *pubDate* and *title*,
    name the corresponding columns *rss_link*, *rss_pubdate* and *rss_title* in
    a database table *items*, which will use the columns *rss_link* and
    *rss_title* as compound primary key (unless the database already exists).

        urls = ["https://www.finanznachrichten.de/rss-nachrichten-meistgelesen",
                "https://www.finanznachrichten.de/rss-marktberichte"]
        feeds_to_sqlite(urls, "db/foo.db", tablename="items",
                          tags={"link": "rss_link", "pubDate": "rss_pubdate", "title": "rss_title"},
                          keys=["rss_link", "rss_title"])
    """
    df = feeds_to_dataframe(urls, tags)
    columns = list(df)  # retrieve column names
    # create path to db
    path = Path(dbpath)
    if not path.is_file():
        path.parent.mkdir(parents=True, exist_ok=True)
        # construct instruction to create table based on given column names. E.g.
        # CREATE TABLE items (guid TEXT, link TEXT, ..., PRIMARY KEY (guid, link))
        create_table_instruction = f"CREATE TABLE {tablename} (" \
            + " TEXT, ".join(columns + [""]) \
            + " PRIMARY KEY (" \
            + ", ".join(keys) \
            + "))"
        conn = sqlite3.connect(str(path))
        conn.execute(create_table_instruction)
    else:
        conn = sqlite3.connect(str(path))
    # construct instruction to insert records into table. E.g
    # INSERT OR IGNORE INTO items (guid, link) VALUES (?, ?)
    insert_instruction = f"INSERT OR IGNORE INTO {tablename} (" + ", ".join(columns) + ") VALUES (" \
        + ("?, " * len(columns)).rstrip(", ") + ")"
    conn.executemany(insert_instruction, list(df.itertuples(index=False, name=None)))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    urls = ["https://lukesmith.xyz/rss.xml", "https://www.finanznachrichten.de/rss-nachrichten-meistgelesen"]
    # tags = {"link": "lelink", "guid": "rss_guid", "title": "rss_title"}
    # keys = ["rss_guid", "rss_title"]
    # print(feeds_to_dataframes(urls, tags=tags).to_csv())
    # archive_rss_feed(urls, "db/rss.db", "items", tags, keys)
    feeds_to_database(urls, "db/foo.db", tablename="items",
                      tags={"link": "rss_link", "pubDate": "rss_pubdate", "title": "rss_title"},
                          keys=["rss_link", "rss_title"])
