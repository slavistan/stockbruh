"""RSS processing logic."""
from pathlib import Path
import logging
import sqlite3

import bs4

import pandas as pd

import requests


def get_rss_feed(urls: list) -> pd.DataFrame:
    """Download RSS feeds and return as dataframe.

    Parameters
    ----------
    urls: list of str
        URL strings, e.g. https://lukesmith.xyz/rss.xml

    Returns
    -------
    pandas.DataFrame
        Columns: link, guid, pubDate, title, description.
    """
    df = pd.DataFrame(columns=["link", "guid", "pubDate", "title", "description"], dtype="string")
    for url in urls:
        response = requests.get(url)
        if response.status_code == 200:
            soup = bs4.BeautifulSoup(response.text, 'xml').find("rss")
            if soup is not None:
                for item in soup.find_all("item"):
                    link = item.link.text if item.link is not None else ""
                    guid = item.guid.text if item.guid is not None else ""
                    title = item.title.text if item.title is not None else ""
                    description = item.description.text if item.description is not None else ""
                    pubdate = item.pubDate.text if item.pubDate is not None else ""
                    df = df.append({"link": link, "guid": guid, "pubDate": pubdate, "title": title,
                                   "description": description}, ignore_index=True)
        else:
            logging.error(f"Download of RSS feed {url} failed.")
    return df


def archive_rss_feed(urls: list, dbpath: str, tablename="items"):
    """Download RSS feeds and store to sqlite database.

    Parameters
    ----------
    urls: list of str
        URL strings, e.g. https://lukesmith.xyz/rss.xml
    dbpath: str
        Relative or absolute path to sqlite database. Database will be created if it does not exist.
    tablename: str
        Name of database table where information will be stored.
    """
    # create path to db
    path = Path(dbpath)
    if not path.is_file():
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path))
        conn.execute(f"CREATE TABLE {tablename} ("
                     "guid TEXT,"
                     "link TEXT,"
                     "pubDate TEXT,"
                     "title TEXT,"
                     "description TEXT,"
                     "PRIMARY KEY (guid, link));")
    else:
        conn = sqlite3.connect(str(path))
    df = get_rss_feed(urls)
    for _, row in df.iterrows():
        conn.execute(f"INSERT OR IGNORE INTO {tablename} (guid, link, pubDate, title, description)"
                     " VALUES (?, ?, ?, ?, ?)",
                     (row["guid"], row["link"], row["pubDate"], row["title"], row["description"]))
    conn.commit()
    conn.close()



if __name__ == "__main__":
    # print(get_rss_feed(["https://www.finanznachrichten.de/rss-aktien-nachrichten"]).to_csv())
    archive_rss_feed(["https://lukesmith.xyz/rss.xml", "https://www.finanznachrichten.de/rss-aktien-nachrichten"], "db/rss.db")
