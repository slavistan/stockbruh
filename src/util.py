"""Utility functions."""
import logging
import sqlite3
from pathlib import Path


def create_db(dbpath, schemapath=None):
    """If sqlite database *dbpath* does not exist create it and execute SQL instructions from file.

    Parameters
    ----------
    dbpath: str
        Path to database. All subdirectories and the database will be created
        if not existing.

    schemapath: str or None
        Path to file containing SQL instructions or 'None', if no instructions
        are to be executed. No SQL instructions will be executed it database
        already exists.
    """
    dbpath = Path(dbpath)
    if dbpath.is_file():
        logging.debug(f"File '{dbpath}' exists. Nothing to do.")
        return

    # Create directories and database
    dbpath.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(dbpath))
    logging.debug(f"Created database '{str(dbpath)}'.")

    # Execute SQL from file
    if schemapath is not None:
        with open(schemapath) as f:
            cur = conn.cursor()
            cur.executescript(f.read())
            logging.debug(f"Executed SQL in '{schemapath}'.")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_db("/tmp/bingo.db", "./db/rss-feeds.schema")
