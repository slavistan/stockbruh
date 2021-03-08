"""Utility functions."""
import argparse

import logging
log = logging.getLogger("stockbro")

import sqlite3
from pathlib import Path


# Realistic user agent to use for requests
USERAGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4340.112 Safari/537.36"


def create_db(dbpath, schemafile=None):
    """Create database and execute SQL instructions from file.

    If sqlite database `dbpath` does not exist, it will be created. If, in
    addition, a `schemafile` is provided the file's contents are executed as
    SQL instructions. If the database exists calls to this function are a
    no-op. Use this function to create and preconfigure sqlite databases.

    Parameters
    ----------
    dbpath: str
        Path to sqlite3 database. All subdirectories and the database will be
        created if not existing.

    schemapath: str or None
        Path to file containing SQL instructions or 'None', if no instructions
        are to be executed. No SQL instructions will be executed it database
        already exists.
    """
    dbpath = Path(dbpath)
    if dbpath.is_file():
        log.debug(f"File '{dbpath}' exists. Nothing to do.")
        return

    # Create directories and database
    dbpath.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(dbpath))
    log.debug(f"Created database '{str(dbpath)}'.")

    # Execute SQL from file
    if schemafile is not None:
        with open(schemafile) as f:
            cur = conn.cursor()
            cur.executescript(f.read())
            log.debug(f"Executed SQL in '{schemafile}'.")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_db("/tmp/bingo.db", "./db/rss-feeds.schema")
