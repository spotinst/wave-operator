#!/usr/bin/env python3

import sys
import os
import requests
import time
from os.path import isdir, join

source_dir = sys.argv[1]
target_dir = sys.argv[2]

frequency = "forever"
if len(sys.argv) > 3:
    frequency = sys.argv[3]

SYNC_INTERVAL_SECONDS = 5
V1_PREFIX = "spark-"
V2_PREFIX = "eventlog_v2_"

print("running sync on {} -> {} {}\n".format(source_dir, target_dir, frequency), flush=True)


class CredentialProvider:
    # use python requests to get credentials from the v1 IMDS, because rclone
    # uses the aws-sdk-go, and has very slow responses
    # see https://github.com/aws/aws-sdk-go/issues/2972

    def __init__(self):
        self.cred_url = "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
        r = requests.get(self.cred_url)
        self.instance_role = r.text

    def load(self):
        r = requests.get(self.cred_url + self.instance_role)
        return r.json()


region_arg = ""
region = os.getenv("S3_REGION")
if region is not None:
    region_arg = """--s3-region {}""".format(region)

provider = CredentialProvider()


def get_credential_arg():
    c = provider.load()
    return """--s3-access-key-id {} --s3-secret-access-key {} --s3-session-token {}""".format(c["AccessKeyId"], c["SecretAccessKey"], c["Token"])


def sync():
    files = os.listdir(source_dir)
    print("""Current files: {}""".format(files), flush=True)
    is_v1 = any(f.startswith(V1_PREFIX) for f in files)
    is_v2 = any(f.startswith(V2_PREFIX) for f in files)
    if is_v1 and not is_v2:
        return sync_v1(files)
    elif is_v2 and not is_v1:
        return sync_v2(files)
    else:
        print("Could not determine event log version", flush=True)
        return False


def sync_v1(files):
    print("Syncing V1 event logs ...", flush=True)
    is_final = any(is_final_v1(f) for f in files)
    if is_final:
        credential_arg = get_credential_arg()
        # use "copy", not "sync": don't delete files in target directory
        command = """rclone {} copy {} {} --progress --local-no-check-updated""".format(region_arg, source_dir, target_dir)
        print(command, flush=True)
        command_with_credentials = """{} {}""".format(command, credential_arg)
        exit_code = os.WEXITSTATUS(os.system(command_with_credentials))
        if exit_code == 0:
            # Sync done
            return True
        else:
            print("""Got exit code: {}""".format(exit_code), flush=True)
            return False
    else:
        print("Log file not finalized, ignoring ...", flush=True)
        return False


def sync_v2(files):
    print("Syncing V2 event logs ...", flush=True)

    event_log_dirs = [f for f in files if isdir(join(source_dir, f)) and f.startswith(V2_PREFIX)]
    if len(event_log_dirs) != 1:
        print("Could not find event log dir", flush=True)
        return False

    event_log_dir = event_log_dirs[0]
    event_log_dir_path = join(source_dir, event_log_dir)
    print("""Syncing event log dir: {}""".format(event_log_dir_path), flush=True)
    dir_files = os.listdir(event_log_dir_path)
    print("""Event log dir files: {}""".format(dir_files), flush=True)

    exclusion_arg = ""
    is_final = any(is_final_v2(f) for f in dir_files)
    if not is_final:
        active_log_file_index = get_active_log_file_index(dir_files)
        if active_log_file_index != "":
            exclusion_arg = """--exclude events_{}_*""".format(active_log_file_index)
        else:
            print("Could not determine active log file index", flush=True)

    credential_arg = get_credential_arg()
    target_event_log_dir_path = join(target_dir, event_log_dir)

    # Since V2 logs go in their own folder, we can use sync instead of copy
    command = """rclone {} sync {} {} {} --progress --local-no-check-updated""".format(region_arg, event_log_dir_path, target_event_log_dir_path, exclusion_arg)
    print(command, flush=True)
    command_with_credentials = """{} {}""".format(command, credential_arg)
    exit_code = os.WEXITSTATUS(os.system(command_with_credentials))

    if exit_code == 0:
        if is_final:
            return True
        else:
            return False
    else:
        print("""Got exit code: {}""".format(exit_code), flush=True)
        return False


def is_final_v1(filename):
    if filename.startswith(V1_PREFIX) and not filename.endswith(".inprogress"):
        return True
    else:
        return False


def is_final_v2(filename):
    if filename.startswith("appstatus_") and not filename.endswith(".inprogress"):
        return True
    else:
        return False


def get_active_log_file_index(files):
    event_files = [f for f in files if f.startswith("events_")]
    if len(event_files) == 0:
        print("No event log files found", flush=True)
        return ""
    indices = [f.split("_")[1] for f in event_files]
    indices = [i for i in indices if i.isnumeric()]  # Make sure we only deal with numbers
    if len(indices) == 0:
        print("No event log file indices found", flush=True)
        return ""
    return max(indices)


if frequency == "forever":
    print("""Running sync every {} seconds""".format(SYNC_INTERVAL_SECONDS), flush=True)
    while True:
        done = sync()
        if done is True:
            print("Sync done, will exit", flush=True)
            exit(0)
        time.sleep(SYNC_INTERVAL_SECONDS)
else:
    print("Running sync once", flush=True)
    sync()

