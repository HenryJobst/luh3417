from datetime import datetime
import re
from argparse import Namespace, ArgumentParser
from os.path import join
from typing import Optional, Sequence, Dict

from luh3417.luhfs import Location, parse_location
from luh3417.luhsql import create_from_source
from luh3417.restore import read_config
from luh3417.utils import setup_logging, make_doer, run_main

doing = make_doer("luh3417.backup_database")


def parse_args(args: Optional[Sequence[str]] = None) -> Namespace:
    """
    Parse arguments fro the snapshot
    """

    parser = ArgumentParser(
        description=(
            "Takes a snapshot of a mysql database. This "
            "requires mysqldump"
        )
    )

    parser.add_argument(
        "-p", "--patch", help="A settings patch file", type=parse_location, default="settings.json"
    )

    parser.add_argument(
        "source",
        help=(
            "Source directory for the database login information dir. Syntax: "
            "`/var/www` or `user@host:/var/www`"
        ),
        type=parse_location,
    )

    parser.add_argument(
        "backup_dir", help="Directory to store the snapshot", type=parse_location
    )

    parser.add_argument(
        "-n",
        "--snapshot-base-name",
        help="Base name for the snapshot file. Defaults to DB name.",
    )

    parser.add_argument(
        "-t",
        "--file-name-template",
        help="Template for snapshot file name. Defaults to: `{base}_dump_{time}.sql`",
        default="{base}_dump_{time}.sql",
    )

    parser.add_argument(
        "-c",
        "--compression-mode",
        help="Compression mode for tar (gzip, bzip2, lzip, xz). Defaults to: gzip",
        default="gzip",
        const="gzip",
        nargs="?",
        choices=["gzip", "bzip2", "lzip", "xz"],
    )

    parser.add_argument(
        "--db-host",
        help=(
            "Optional IP address of the database server, if IP of the wpconfig.php is a local one."
        ),
        default=None,
        const=None,
        nargs="?",
    )

    parsed_args = parser.parse_args(args)

    # apply compression mode to file name template
    if parsed_args.compression_mode == "bzip2":
        parsed_args.file_name_template = re.sub(".gz", ".bz2", parsed_args.file_name_template)
        parsed_args.backup_dir.set_compression_mode(parsed_args.compression_mode)
    elif parsed_args.compression_mode == "lzip":
        parsed_args.file_name_template = re.sub(".gz", ".lz", parsed_args.file_name_template)
        parsed_args.backup_dir.set_compression_mode(parsed_args.compression_mode)
    elif parsed_args.compression_mode == "xz":
        parsed_args.file_name_template = re.sub(".gz", ".xz", parsed_args.file_name_template)
        parsed_args.backup_dir.set_compression_mode(parsed_args.compression_mode)

    return parsed_args


def make_dump_file_name(args: Namespace, wp_config: Dict, now: datetime) -> Location:
    """
    Generates the location name where to dump the file
    """

    if not args.snapshot_base_name:
        base_name = wp_config["db_name"]
    else:
        base_name = args.snapshot_base_name

    name = args.file_name_template.format(base=base_name, time=now.isoformat() + "Z")

    return args.backup_dir.child(name)


def main(args: Optional[Sequence[str]] = None):

    setup_logging()
    args = parse_args(args)
    now = datetime.utcnow()

    with doing("Reading configuration"):
        config = read_config(join(args.source.path, args.patch.path))

    with doing("Dump database"):
        args.backup_dir.ensure_exists_as_dir()
        db = create_from_source(config['wp_config'], args.source, args.db_host)
        archive_location = make_dump_file_name(args, config['wp_config'], now)
        db.dump_to_file(archive_location.path)

    return archive_location


if __name__ == "__main__":
    run_main(main, doing)
