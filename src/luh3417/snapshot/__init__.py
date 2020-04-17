import subprocess
from typing import Sequence, Text

from luh3417.luhfs import LocalLocation, Location, SshLocation
from luh3417.luhssh import SshManager
from luh3417.utils import LuhError


def sync_files(remote: Location, local: Location, delete: bool = False):
    """
    Use rsync to copy files from a location to another
    """

    local.ensure_exists_as_dir()

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

    args += [remote.rsync_path(True), local.rsync_path(True)]

    cp = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    if cp.returncode:
        raise LuhError(f"Error while copying files: {cp.stderr}")


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


def copy_files(remote: Location, local: Location, excludes, exclude_tag_alls):
    """
    Copies files from the remote location to the local locations. Files are
    serialized and pipelined through tar, maybe locally, maybe through SSH
    depending on the locations.
    """

    remote_tar_command = ["tar", "-C", remote.path]
    if excludes:
        for exclude in excludes:
            remote_tar_command.append("--exclude")
            remote_tar_command.append(exclude)
    if exclude_tag_alls:
        for exclude_tag_all in exclude_tag_alls:
            remote_tar_command.append("--exclude-tag-all")
            remote_tar_command.append(exclude_tag_all)
    remote_tar_command.extend(["-c", "."])

    remote_args = _build_args(remote, remote_tar_command)
    local_args_1 = _build_args(local, ["mkdir", "-p", local.path])
    local_args_2 = _build_args(local, ["tar", "-C", local.path, "-x"])

    cp = subprocess.run(local_args_1, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    if cp.returncode:
        raise LuhError(f'Error while creating target dir "{local}": {cp.stderr}')

    remote_p = subprocess.Popen(
        remote_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    local_p = subprocess.Popen(
        local_args_2,
        stdin=remote_p.stdout,
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
    )

    remote_p.wait()
    local_p.wait()

    if remote_p.returncode:
        raise LuhError(
            f'Error while reading files from "{remote}": {remote_p.stderr.read(1000)}'
        )

    if local_p.returncode:
        raise LuhError(f'Error writing files to "{local}": {local_p.stderr.read(1000)}')
