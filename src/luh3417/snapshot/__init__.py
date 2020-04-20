import subprocess
import re

from typing import Sequence, Text

from luh3417.luhfs import LocalLocation, Location, SshLocation
from luh3417.luhssh import SshManager
from luh3417.utils import LuhError


def rsync_files(source: Location, target: Location, delete: bool = False):
    """
    Use rsync to copy files from a location to another
    """

    args = [
        "rsync",
        "-rz",
        "--exclude=.git",
        "--exclude=.idea",
        "--exclude=*.swp",
        "--exclude=*.un~",
    ]

    if delete:
        args.append("--delete")

    args += [source.rsync_path(True), target.rsync_path(True)]

    cp = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    return cp.returncode, cp.stderr


def sync_files(source: Location, target: Location, delete: bool = False):
    """
    Use rsync to copy files from a location to another
    """

    target.ensure_exists_as_dir()

    rc, stderr = rsync_files(source, target, delete)

    if rc:
        cmd_not_found = re.search("command not found", str(stderr))
        if not cmd_not_found:
            raise LuhError(f"Error while copying files: {stderr}")

        copy_files_with_delete(source, target, delete)


def _build_args(location: Location, args: Sequence[Text]) -> Sequence[Text]:
    """
    Builds args to use either with SSH either straight
    """

    if isinstance(location, LocalLocation):
        return args
    elif isinstance(location, SshLocation):
        return SshManager.instance(location.user, location.host, location.port).get_args(args)


def activate_maintenance_mode(remote: Location):
    remote_args = _build_args(remote, ["wp", "maintenance-mode", "activate", "--path=", remote.path, "--quiet"])

    remote_p = subprocess.Popen(
        remote_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    remote_p.wait()

    if remote_p.returncode:
        raise LuhError(
            f'Error while activate maintenance mode at "{remote}": {remote_p.stderr.read(1000)}'
        )


def deactivate_maintenance_mode(remote: Location):
    remote_args = _build_args(remote, ["wp", "maintenance-mode", "deactivate", "--path=", remote.path, "--quiet"])

    remote_p = subprocess.Popen(
        remote_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    remote_p.wait()

    if remote_p.returncode:
        raise LuhError(
            f'Error while deactivate maintenance mode at "{remote}": {remote_p.stderr.read(1000)}'
        )


def copy_files(source: Location, target: Location, excludes, exclude_tag_alls):
    """
    Copies files from the remote location to the local locations. Files are
    serialized and pipelined through tar, maybe locally, maybe through SSH
    depending on the locations.
    """

    source_tar_command = ["tar", "-C", source.path]
    if excludes:
        for exclude in excludes:
            source_tar_command.append("--exclude")
            source_tar_command.append(exclude)
    if exclude_tag_alls:
        for exclude_tag_all in exclude_tag_alls:
            source_tar_command.append("--exclude-tag-all")
            source_tar_command.append(exclude_tag_all)
    source_tar_command.extend(["-c", "."])

    source_args = _build_args(source, source_tar_command)
    target_args_1 = _build_args(target, ["mkdir", "-p", target.path])
    target_args_2 = _build_args(target, ["tar", "-C", target.path, "-x"])

    cp = subprocess.run(target_args_1, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    if cp.returncode:
        raise LuhError(f'Error while creating target dir "{target}": {cp.stderr}')

    source_p = subprocess.Popen(
        source_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    target_p = subprocess.Popen(
        target_args_2,
        stdin=source_p.stdout,
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
    )

    source_p.wait()
    target_p.wait()

    if source_p.returncode:
        raise LuhError(
            f'Error while reading files from "{source}": {source_p.stderr.read(1000)}'
        )

    if target_p.returncode:
        raise LuhError(f'Error writing files to "{target}": {target_p.stderr.read(1000)}')


def copy_files_with_delete(source: Location, target: Location, delete: bool = False):

    if delete:
        target.delete_dir_content()
        target.ensure_exists_as_dir()

    copy_files(source, target, None, None)
