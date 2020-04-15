import json
from argparse import ArgumentParser, ArgumentTypeError, Namespace
from os.path import exists
from tempfile import NamedTemporaryFile
from typing import Optional, Sequence, Text

from luh3417.luhfs import parse_location
from luh3417.luhphp import parse_wp_config
from luh3417.restore.__main__ import main as restore
from luh3417.snapshot.__main__ import main as snapshot
from luh3417.transfer import UnknownEnvironment, apply_wp_config
from luh3417.utils import import_file, make_doer, run_main, setup_logging

doing = make_doer("luh3417.transfer")


def generator_validator(file_path: Text):
    """
    Validates that the generator module exists and has all the required
    methods
    """

    if not exists(file_path):
        raise ArgumentTypeError(f"File {file_path} could not be found")

    try:
        module = import_file("generator", file_path)
    except SyntaxError:
        doing.logger.exception("Syntax error in generator")
        raise ArgumentTypeError(f"File {file_path} has a syntax error")
    except ImportError:
        raise ArgumentTypeError(f"File {file_path} cannot be imported")
    except Exception:
        doing.logger.exception("Unknown error while importing generator")
        raise ArgumentTypeError(f"Unknown error")

    if not hasattr(module, "get_source") or not callable(module.get_source):
        raise ArgumentTypeError(
            f"Generator does not expose a get_source(environment_name) method"
        )

    if not hasattr(module, "allow_transfer") or not callable(module.allow_transfer):
        raise ArgumentTypeError(
            f"Generator does not expose a allow_transfer(origin, target) method"
        )

    if not hasattr(module, "get_backup_dir") or not callable(module.get_backup_dir):
        raise ArgumentTypeError(
            f"Generator does not expose a get_backup_dir(environment) method"
        )

    if not hasattr(module, "get_patch") or not callable(module.get_backup_dir):
        raise ArgumentTypeError(
            f"Generator does not expose a get_patch(origin, target) method"
        )

    if not hasattr(module, "get_wp_config") or not callable(module.get_backup_dir):
        raise ArgumentTypeError(
            f"Generator does not expose a get_wp_config(environment) method"
        )

    return module


def parse_args(args: Optional[Sequence[str]] = None) -> Namespace:
    """
    Parses args and validates the consistency of origin/target using the
    generator
    """

    parser = ArgumentParser(
        prog="python -m luh3417.transfer",
        description="Transfers a WordPress to one location to the other",
    )

    parser.add_argument(
        "-g",
        "--settings-generator",
        help="A Python script that handles the transitions",
        type=generator_validator,
        required=True,
    )

    parser.add_argument("origin", help="Origin environment")
    parser.add_argument("target", help="Target environment")

    parsed = parser.parse_args(args)

    for env in ["origin", "target"]:
        env_name = getattr(parsed, env)

        try:
            parsed.settings_generator.get_source(env_name)
        except UnknownEnvironment as e:
            parser.error(
                f'Environment "{env_name}" not recognized by generator: {e.message}'
            )

    if not parsed.settings_generator.allow_transfer(parsed.origin, parsed.target):
        parser.error(
            f"Generator does not allow transfer from {parsed.origin} to {parsed.target}"
        )

    return parsed


def main(args: Optional[Sequence[str]] = None):
    """
    Executes things in order
    """

    setup_logging()
    args = parse_args(args)

    gen = args.settings_generator

    origin_source = parse_location(gen.get_source(args.origin), args.compression_mode)
    origin_backup_dir = gen.get_backup_dir(args.origin)

    with doing(f"Backing up {args.origin} to {origin_backup_dir}"):
        origin_archive = snapshot([f"{origin_source}", origin_backup_dir])

    target_backup_dir = gen.get_backup_dir(args.target)
    target_source = parse_location(gen.get_source(args.target), args.compression_mode)

    with doing(f"Checking if {args.target} ({target_source}) already exists"):
        target_exists = target_source.exists()

    if target_exists:
        with doing(f"Backing up {args.target} to {target_backup_dir}"):
            snapshot([f"{target_source}", target_backup_dir])

    if target_exists:
        with doing(f"Reading wp_config from {args.target}"):
            wp_config = parse_wp_config(target_source)
    else:
        with doing(f"Generating wp_config for {args.target}"):
            wp_config = gen.get_wp_config(args.target)

    patch = apply_wp_config(
        gen.get_patch(args.origin, args.target), wp_config, target_source
    )

    with NamedTemporaryFile(mode="w", encoding="utf-8") as pf:
        json.dump(patch, pf)
        pf.flush()

        with doing(f"Overriding {args.target} with {args.origin}"):
            restore(["-p", pf.name, f"{origin_archive}"])

    if hasattr(gen, "post_exec"):
        with doing("Running post-exec hook"):
            gen.post_exec(args.origin, args.target)


if __name__ == "__main__":
    run_main(main, doing)
