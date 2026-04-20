"""Shared SFTP download helper for credit report scripts.

Both credit_report.py and credit_report_gfactor.py pull participant data
files from the Sussex server using the same credentials and download logic.
This module centralises that.
"""

import os
import paramiko

SSH_HOST = "unix.sussex.ac.uk"


def download_from_paths(remote_paths, local_dir, since=None,
                        file_suffix='.json'):
    """SFTP files from one or more remote folders into local_dir.

    Args:
        remote_paths: list of remote folder paths to scan.
        local_dir: where to save downloads.
        since: optional datetime. If set, skip files whose remote mtime is
               before this date.
        file_suffix: only download files ending with this suffix.

    Skips any file where the local copy is already up-to-date
    (same size + local mtime >= remote mtime), so repeat runs are fast.

    Credentials come from env vars SUSSEX_USER and SUSSEX_PASS.
    """
    user = os.environ['SUSSEX_USER']
    pw = os.environ['SUSSEX_PASS']

    os.makedirs(local_dir, exist_ok=True)

    since_ts = since.timestamp() if since else None
    since_str = f" (only files modified on/after {since.date()})" if since else ""
    print(f"Connecting to {SSH_HOST} as {user}{since_str}...")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(SSH_HOST, username=user, password=pw)

    sftp = ssh.open_sftp()
    n_downloaded = n_too_old = n_already_local = 0

    try:
        for remote_path in remote_paths:
            print(f"  Checking {remote_path}")
            try:
                entries = sftp.listdir_attr(remote_path)
            except FileNotFoundError:
                print(f"  WARNING: remote folder not found: {remote_path}")
                continue

            matching = [e for e in entries if e.filename.endswith(file_suffix)]
            folder_downloaded = 0

            for entry in matching:
                fname = entry.filename
                remote_mtime = entry.st_mtime
                remote_size = entry.st_size

                if since_ts is not None and remote_mtime < since_ts:
                    n_too_old += 1
                    continue

                local_file = os.path.join(local_dir, fname)

                if os.path.exists(local_file):
                    local_stat = os.stat(local_file)
                    if (local_stat.st_size == remote_size
                            and local_stat.st_mtime >= remote_mtime):
                        n_already_local += 1
                        continue

                remote_file = remote_path.rstrip('/') + '/' + fname
                sftp.get(remote_file, local_file)
                # Preserve remote mtime so future runs can skip this file
                os.utime(local_file, (remote_mtime, remote_mtime))
                n_downloaded += 1
                folder_downloaded += 1

            print(f"    {len(matching)} {file_suffix} files in folder, "
                  f"{folder_downloaded} newly downloaded")
    finally:
        sftp.close()
        ssh.close()

    print(f"Download summary: {n_downloaded} new, "
          f"{n_already_local} already current, "
          f"{n_too_old} before --since cutoff")