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
    formatter_class = argparse.ArgumentDefaultsHelpFormatter
    """Parse command-line arguments and return populated Namespace object."""
    parser = argparse.ArgumentParser(formatter_class=formatter_class)
    parser.add_argument("-l", "--loglevel", default="info", choices=["debug", "info", "warning", "error", "fatal"],
                        help="Set minimum importance threshold for console logging output. Case insensitive.")
    subparsers = parser.add_subparsers(dest="command")
    rss = subparsers.add_parser("rss", formatter_class=formatter_class)
    rss_subparsers = rss.add_subparsers(dest="rss_command")
    rss_fetch = rss_subparsers.add_parser("fetch", formatter_class=formatter_class)
    rss_download = rss_subparsers.add_parser("download", formatter_class=formatter_class)
    rss_extract = rss_subparsers.add_parser("extract", formatter_class=formatter_class)

    # configure subcommand
    parser_fetch_rss_feeds = subparsers.add_parser("rss-fetch", formatter_class=formatter_class)

    parser_download_html = subparsers.add_parser("rss-download-html", formatter_class=formatter_class)
    parser_download_html.add_argument("-m", "--maxitems", type=int, default=32,  # FIXME: enforce nonneg integers
                                      help="Stop after given number of items have been processed. Used to chunk up "
                                           "workload into batches of predictable duration.")

    parser_extract_fulltext = subparsers.add_parser("rss-extract-fulltext", formatter_class=formatter_class)
    parser_extract_fulltext.add_argument("-m", "--maxitems", type=int, default=32,  # FIXME: enforce nonneg integers
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

    logpath.parent.mkdir(parents=True, exist_ok=True)

    # Root logger setup. Writes ALL logging output to file including logging
    # from imported modules.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(str(logpath))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(name)s - %(funcName)s: %(message)s"))
    root_logger.addHandler(fh)

    # Module-level logger. Writes this project's logging output to console.
    log = logging.getLogger("stockbro")
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter("[%(levelname)s] %(funcName)s: %(message)s"))
    log.addHandler(ch)


if __name__ == "__main__":

    # Load static configuration parameters.
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Parse command-line arguments.
    args = init_args(sys.argv[1:])

    # Configure logging. One logfile per day, e.g. 'log/2021-03-12.log'.
    today = str(datetime.now().strftime(r"%Y-%m-%d"))
    logpath = pathlib.Path(config["project"]["logdir"]) / f"{today}.log"
    loglevel = logging.getLevelName(args.loglevel.upper())
    init_logging(logpath, loglevel)
    log = logging.getLogger("stockbro")

    # Dispatch commands.
    if args.command == "rss":
        if args.rss_command == "fetch":
            log.info("rss fetch!")
        elif args.rss_command == "download":
            log.info("rss download!")
        elif args.rss_command == "extract":
            log.info("rss extract!")
