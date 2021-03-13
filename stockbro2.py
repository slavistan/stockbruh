#!/usr/bin/env python3
"""Main CLI modules."""

import argparse
import configparser
import logging
import sqlite3
import pathlib
import sys

import yaml

from datetime import datetime
from pathlib import Path

import requests

from src import rss
from src import util


def init_args(argv: list) -> argparse.Namespace:
    """Parse command-line arguments and return populated Namespace object."""
    # Use a default formatter which displays default values in help string.
    formatter_class = argparse.ArgumentDefaultsHelpFormatter

    parser = argparse.ArgumentParser(formatter_class=formatter_class)
    parser.add_argument("-l", "--loglevel", default="info", choices=["debug", "info", "warning", "error", "critical"],
                        help="Set minimum importance threshold for console logging output. Case insensitive.")
    subparsers = parser.add_subparsers(dest="command")
    rss = subparsers.add_parser("rss", formatter_class=formatter_class)
    rss_subparsers = rss.add_subparsers(dest="rss_command")
    rss_fetch = rss_subparsers.add_parser("fetch", formatter_class=formatter_class)
    rss_fetch.add_argument("-m", "--maxitems", type=int, default=32,  # FIXME: enforce nonneg integers
                           help="Stop after given number of items have been processed. Used to chunk up "
                                "workload into batches of predictable duration.")
    rss_download = rss_subparsers.add_parser("download", formatter_class=formatter_class)
    rss_download.add_argument("-m", "--maxitems", type=int, default=32,  # FIXME: enforce nonneg integers
                              help="Stop after given number of items have been processed. Used to chunk up "
                                   "workload into batches of predictable duration.")
    rss_extract = rss_subparsers.add_parser("extract", formatter_class=formatter_class)
    rss_extract.add_argument("-m", "--maxitems", type=int, default=32,  # FIXME: enforce nonneg integers
                             help="Stop after given number of items have been processed. Used to chunk up "
                                  "workload into batches of predictable duration.")

    return parser.parse_args(argv)


def init_logging(logpath: pathlib.Path, level=logging.INFO):
    """Configure logging for this project.

    All logging output including that from imported modules is saved to file at
    DEBUG-level verbosity. Additionally, logging output of this project's own
    modules is sent to console. The console logger's verbosity can be adjusted
    via the level parameter.

    After this function has been called any dependent module shall initialize and
    use logging like so:

        import logging
        log = logging.getLogger("stockbro")
        log.info("...")

    Parameters
    ----------
    logpath: pathlib.Path
        Relative or absolute path of logfile. File including subdirectories
        will be created.

    level: int
        Verbosity of console logging. Logging to file always uses logging.DEBUG.
    """

    class ConsoleFormatter(logging.Formatter):
        """Logging formatter to add colors to terminal output."""

        whitebold = "\x1b[0;37;1m"
        greenbold = "\x1b[0;32;1m"
        yellowbold = "\x1b[0;33;1;21m"
        redbold = "\x1b[0;31;1;21m"
        reset = "\x1b[0m"
        level = "%(levelname).4s"
        suffix = " %(asctime)s.%(msecs)03d %(message)s"

        FORMATS = {
            logging.DEBUG: whitebold + level + reset + suffix,
            logging.INFO: greenbold + level + reset + suffix,
            logging.WARNING: yellowbold + level + reset + suffix,
            logging.ERROR: redbold + level + reset + suffix,
            logging.CRITICAL: redbold + level + reset + suffix
        }

        def format(self, record):
            log_fmt = self.FORMATS.get(record.levelno)
            formatter = logging.Formatter(log_fmt, "%H:%M:%S")
            return formatter.format(record)

    logpath.parent.mkdir(parents=True, exist_ok=True)

    # Root logger setup. Writes ALL logging output to file including logging
    # from imported modules.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(str(logpath))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(levelname).4s %(asctime)s.%(msecs)03d "
                                      "%(name)s@%(filename)s:%(lineno)d %(message)s",
                                      "%Y/%m/%d %H:%M:%S"))
    root_logger.addHandler(fh)

    # Module-level logger. Writes this project's logging output to console.
    log = logging.getLogger("stockbro")
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(ConsoleFormatter())
    log.addHandler(ch)


if __name__ == "__main__":

    # Load static configuration parameters.
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Parse command-line arguments.
    args = init_args(sys.argv[1:])

    # Configure logging. One logfile per day, e.g. 'log/2021-03-12.log'.
    today = datetime.now().strftime(r"%Y-%m-%d")
    logpath = pathlib.Path(config["project"]["logdir"]) / f"{today}.log"
    loglevel = logging.getLevelName(args.loglevel.upper())
    init_logging(logpath, loglevel)
    log = logging.getLogger("stockbro")

    # Dispatch commands.
    if args.command == "rss":
        if args.rss_command == "fetch":
            urls = config["rss"]["feeds"]

            # Convert paths from Posix used in the config to whatever the OS is using.
            feedsdb_path = pathlib.Path(pathlib.PurePosixPath(config["rss"]["feedsdb-path"]))
            feedsdb_schema = pathlib.Path(pathlib.PurePosixPath(config["rss"]["feedsdb-schema"]))

            # Count rows in database table before insertion
            util.create_db(feedsdb_path, feedsdb_schema)
            log.info(f"Fetching {len(urls)} RSS feeds ...")
            for ii, url in enumerate(urls, 1):
                try:
                    rss.feeds_to_database([url], feedsdb_path, tablename="items",
                                          tags={"guid": "rss_guid", "link": "rss_link", "pubDate": "rss_pubdate",
                                                "title": "rss_title", "description": "rss_description"},
                                          keys=["rss_guid", "rss_link"])
                    log.info(f"  - {url} ... success.")
                except Exception as e:
                    log.error(f"  - {url} ... error: {e}")

        elif args.rss_command == "download":
            log.debug("rss download!")
            log.info("rss download!")
            log.warning("rss download!")
            log.error("rss download!")
            log.critical("rss download!")
        elif args.rss_command == "extract":
            log.info("rss extract!")
            log.debug("rss extract!")
            log.info("rss extract!")
            log.warning("rss extract!")
            log.error("rss extract!")
            log.critical("rss extract!")
