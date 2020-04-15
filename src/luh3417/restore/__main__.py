from argparse import ArgumentParser, Namespace
from os.path import join
from tempfile import TemporaryDirectory
from typing import Optional, Sequence

from luh3417.luhfs import Location, parse_location
from luh3417.luhphp import set_wp_config_values
from luh3417.luhsql import create_from_source, patch_sql_dump
from luh3417.restore import (
    configure_dns,
    ensure_db_exists,
    get_remote,
    get_wp_config,
    install_outer_files,
    make_replace_map,
    patch_config,
    read_config,
    restore_db,
    restore_files,
    run_post_install,
    run_queries,
)
from luh3417.utils import make_doer, run_main, setup_logging

doing = make_doer("luh3417.restore")


def parse_args(args: Optional[Sequence[str]] = None) -> Namespace:
    """
    Parse arguments from CLI
    """

    parser = ArgumentParser(description="Restores a snapshot")

    parser.add_argument(
        "-p", "--patch", help="A settings patch file", type=parse_location
    )
    parser.add_argument(
        "-a",
        "--allow-in-place",
        help="Allow to restore the backup in-place, overriding its origin",
        action="store_true",
    )

    parser.add_argument(
        "snapshot",
        help=(
            "Location of the snapshot file. Syntax: `~/snap.tar.gz` or "
            "`user@host:snap.tar.gz`"
        ),
        type=parse_location,
    )

    return parser.parse_args(args)


def main(args: Optional[Sequence[str]] = None):
    """
    Executes things in order
    """

    setup_logging()
    args = parse_args(args)
    snap: Location = args.snapshot

    with TemporaryDirectory() as d:
        with doing("Extracting archive"):
            snap.extract_archive_to_dir(d)

        with doing("Reading configuration"):
            config = patch_config(
                read_config(join(d, "settings.json")), args.patch, args.allow_in_place
            )

        dump = join(d, "dump.sql")

        if config["replace_in_dump"]:
            with doing("Patch the SQL dump"):
                new_dump = join(d, "dump_patched.sql")
                patch_sql_dump(
                    dump, new_dump, make_replace_map(config["replace_in_dump"])
                )
                dump = new_dump

        if config["php_define"]:
            with doing("Patch wp-config.php"):
                set_wp_config_values(
                    config["php_define"], join(d, "wordpress", "wp-config.php")
                )

        with doing("Restoring files"):
            remote = get_remote(config)
            restore_files(join(d, "wordpress"), remote)

        if config["git"]:
            with doing("Cloning Git repos"):
                for repo in config["git"]:
                    location = remote.child(repo["location"])
                    location.set_git_repo(repo["repo"], repo["version"])
                    doing.logger.info(
                        "Cloned %s@%s to %s", repo["repo"], repo["version"], location
                    )

        if config["owner"]:
            with doing("Changing files owner"):
                remote.chown(config["owner"])

        with doing("Reading WP config"):
            wp_config = get_wp_config(config)

        if config["mysql_root"]:
            mysql_root = config["mysql_root"]

            with doing("Ensuring that DB and user exist"):
                ensure_db_exists(wp_config, mysql_root, remote)

        with doing("Restoring DB"):
            db = create_from_source(wp_config, remote, args.db_host)
            restore_db(db, dump)

        if config["setup_queries"]:
            with doing("Running setup queries"):
                run_queries(db, config["setup_queries"])

        if config["outer_files"]:
            with doing("Creating outer files"):
                install_outer_files(config["outer_files"], remote)

        if config["post_install"]:
            with doing("Running post install scripts"):
                run_post_install(config["post_install"], remote)

        if config["dns"]:
            with doing("Configuring DNS"):
                configure_dns(config["dns"])


if __name__ == "__main__":
    run_main(main, doing)
