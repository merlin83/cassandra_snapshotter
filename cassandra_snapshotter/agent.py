from __future__ import (absolute_import, print_function)

# From system
from boto.s3.connection import S3Connection
from boto.s3.key import Key
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from yaml import load
try:
    # LibYAML based parser and emitter
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
import os
import time
import glob
import logging
import subprocess
import multiprocessing
from multiprocessing.dummy import Pool

# From package
from .timeout import timeout
from .utils import (add_s3_arguments, base_parser,\
    map_wrap, get_s3_connection_host)


DEFAULT_CONCURRENCY = max(multiprocessing.cpu_count() - 1, 1)
BUFFER_SIZE = 64         # Default bufsize is 64M
MBFACTOR = float(1<<20)
LZOP_BIN = 'lzop'
MAX_RETRY_COUNT = 3
SLEEP_TIME = 2
UPLOAD_TIMEOUT = 600
MULTI_PART_UPLOAD_THRESHOLD = 200000  # If file size > 200G, use multi part upload

logger = logging.getLogger(__name__)


def check_lzop():
    try:
        subprocess.call([LZOP_BIN, '--version'])
    except OSError:
        print("{!s} not found on path".format(LZOP_BIN))


def upload_pipe(path, size, compress):
    """
    Returns a generator that yields compressed chunks of
    the given file_path

    compression is done with lzop

    """
    if compress:
        lzop = subprocess.Popen(
            (LZOP_BIN, '--stdout', path),
            bufsize=size,
            stdout=subprocess.PIPE
        )
        stdout = lzop.stdout
    else:
        stdout = StringIO(open(path, "r"))

    while True:
        chunk = stdout.read(size)
        if not chunk:
            break
        yield StringIO(chunk)


def compress_file(path):
    """
    Return an lzoped file

    Note: This function is not being used at the moment, but will be
    used against single file upload
    """
    subprocess.call([LZOP_BIN, path])
    path = path + '.lzo'
    return path


def get_bucket(
        s3_bucket, aws_access_key_id,
        aws_secret_access_key, s3_connection_host):
    connection = S3Connection(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        host=s3_connection_host
    )
    return connection.get_bucket(s3_bucket, validate=False)


def destination_path(s3_base_path, file_path, compressed=False):
    """
    Set destination file path in AWS S3

    Note:
        For files smaller than 20M, we do not compress
    """
    if os.path.getsize(file_path) <= int(MULTI_PART_UPLOAD_THRESHOLD * MBFACTOR):
        compressed = False
    suffix = compressed and '.lzo' or ''
    return '/'.join([s3_base_path, file_path + suffix])


@map_wrap
def upload_file(bucket, source, destination, s3_ssenc, bufsize, compress):
    completed = False
    retry_count = 0
    # If file size less than MULTI_PART_UPLOAD_THRESHOLD,
    # use single part upload
    if os.path.getsize(source) <= int(MULTI_PART_UPLOAD_THRESHOLD * MBFACTOR):
        print("[SU] {0}".format(source))
        while not completed and retry_count < MAX_RETRY_COUNT:
            try:
                k = Key(bucket)  # Initialize S3 bucket object
                k.key = destination  # Prepend S3 path prior to uploading
                k.set_contents_from_filename(source, encrypt_key=s3_ssenc)
                completed = True
            except Exception:
                print("Error uploading file {!s} to {!s}.\
                    Retry count: {}".format(source, destination, retry_count))
                retry_count = retry_count + 1
                if retry_count >= MAX_RETRY_COUNT:
                    print("Retried too many times uploading file")
                    raise
    else:  # Big file, use multi part upload
        print("[MU] {0}".format(source))
        while not completed and retry_count < MAX_RETRY_COUNT:
            mp = bucket.initiate_multipart_upload(
                destination,
                encrypt_key=s3_ssenc)
            try:
                for i, chunk in enumerate(upload_pipe(source, bufsize, compress)):
                    mp.upload_part_from_file(chunk, i+1)
                mp.complete_upload()  # Finish the upload
                completed = True
            except Exception:
                print("Error uploading file {!s} to {!s}.\
                    Retry count: {}".format(source, destination, retry_count))
                cancel_upload(bucket, mp, destination)
                retry_count = retry_count + 1
                if retry_count >= MAX_RETRY_COUNT:
                    print("Retried too many times uploading file")
                    raise


@timeout(UPLOAD_TIMEOUT)
def upload_chunk(mp, chunk, index):
    mp.upload_part_from_file(chunk, index)


def cancel_upload(bucket, mp, remote_path):
    """
    Safe way to cancel a multipart upload
    sleeps SLEEP_TIME seconds and then makes sure that there are not parts left
    in storage
    """
    while True:
        try:
            time.sleep(SLEEP_TIME)
            mp.cancel_upload()
            time.sleep(SLEEP_TIME)
            for mp in bucket.list_multipart_uploads():
                if mp.key_name == remote_path:
                    mp.cancel_upload()
            return
        except Exception:
            logger.exception("Error while cancelling multipart upload")


def put_from_manifest(
        s3_bucket, s3_connection_host, s3_ssenc, s3_base_path,
        aws_access_key_id, aws_secret_access_key, manifest,
        bufsize, concurrency=None, incremental_backups=False, compress=False):
    """
    Uploads files listed in a manifest to amazon S3
    to support larger than 5GB files multipart upload is used (chunks of 60MB)
    files are uploaded compressed with lzop, the .lzo suffix is appended
    """
    bucket = get_bucket(
        s3_bucket, aws_access_key_id,
        aws_secret_access_key, s3_connection_host)
    manifest_fp = open(manifest, 'r')
    buffer_size = int(bufsize * MBFACTOR)
    files = manifest_fp.read().splitlines()
    pool = Pool(concurrency)
    for _ in pool.imap(upload_file, ((bucket, f, destination_path(s3_base_path, f, compress), s3_ssenc, buffer_size, compress) for f in files)):
        pass
    pool.terminate()

    if incremental_backups:
        for f in files:
            os.remove(f)


def get_data_path(conf_path):
    """Retrieve cassandra data_file_directories from cassandra.yaml"""
    config_file_path = os.path.join(conf_path, 'cassandra.yaml')
    cassandra_configs = {}
    with open(config_file_path, 'r') as f:
        cassandra_configs = load(f, Loader=Loader)
    data_paths = cassandra_configs['data_file_directories']
    return data_paths

def create_upload_manifest(
        snapshot_name, snapshot_keyspaces, snapshot_table,
        conf_path, manifest_path, exclude_tables, incremental_backups=False):
    if snapshot_keyspaces:
        keyspace_globs = snapshot_keyspaces.split()
    else:
        keyspace_globs = ['*']

    if snapshot_table:
        table_glob = snapshot_table
    else:
        table_glob = '*'

    data_paths = get_data_path(conf_path)
    files = []
    exclude_tables_list = exclude_tables.split(',')
    for data_path in data_paths:
        for keyspace_glob in keyspace_globs:
            logger.info("Creating data path: {0}/{1}".format(data_path, keyspace_glob))
            path = [
                data_path,
                keyspace_glob,
                table_glob
            ]
            if incremental_backups:
                path += ['backups']
            else:
                path += ['snapshots', snapshot_name]
            path += ['*']

            path = os.path.join(*path)
            if len(exclude_tables_list) > 0:
                for f in glob.glob(os.path.join(path)):
                    # Get the table name
                    # The current format of a file path looks like:
                    # /var/lib/cassandra/data03/system/compaction_history/snapshots/20151102182658/system-compaction_history-jb-6684-Summary.db
                    if f.split('/')[-4] not in exclude_tables_list:
                        files.append(f.strip())
            else:
                files.append(f.strip() for f in glob.glob(os.path.join(path)))

    with open(manifest_path, 'w') as manifest:
        for f in files:
            manifest.write(f + '\n')


def main():
    subparsers = base_parser.add_subparsers(
        title='subcommands', dest='subcommand')
    base_parser.add_argument(
        '--incremental_backups', action='store_true', default=False)

    put_parser = subparsers.add_parser(
        'put', help="put files on s3 from a manifest")
    manifest_parser = subparsers.add_parser(
        'create-upload-manifest', help="put files on s3 from a manifest")

    # put arguments
    put_parser = add_s3_arguments(put_parser)
    put_parser.add_argument(
        '--bufsize',
        required=False,
        default=BUFFER_SIZE,
        type=int,
        help="Compress and upload buffer size")

    put_parser.add_argument(
        '--manifest',
        required=True,
        help="The manifest containing the files to put on s3")

    put_parser.add_argument(
        '--concurrency',
        required=False,
        default=DEFAULT_CONCURRENCY,
        type=int,
        help="Compress and upload concurrent processes")

    base_parser.add_argument(
        '--compress', action='store_true', default=False)

    # create-upload-manifest arguments
    manifest_parser.add_argument('--snapshot_name', required=True, type=str)
    manifest_parser.add_argument('--conf_path', required=True, type=str)
    manifest_parser.add_argument('--manifest_path', required=True, type=str)
    manifest_parser.add_argument(
        '--snapshot_keyspaces', default='', required=False, type=str)
    manifest_parser.add_argument(
        '--snapshot_table', required=False, default='', type=str)
    manifest_parser.add_argument(
        '--exclude_tables', required=False, type=str)

    args = base_parser.parse_args()
    subcommand = args.subcommand

    if subcommand == 'create-upload-manifest':
        create_upload_manifest(
            args.snapshot_name,
            args.snapshot_keyspaces,
            args.snapshot_table,
            args.conf_path,
            args.manifest_path,
            args.exclude_tables,
            args.incremental_backups
        )

    if subcommand == 'put':
        if args.compress:
            check_lzop()
        put_from_manifest(
            args.s3_bucket_name,
            get_s3_connection_host(args.s3_bucket_region),
            args.s3_ssenc,
            args.s3_base_path,
            args.aws_access_key_id,
            args.aws_secret_access_key,
            args.manifest,
            args.bufsize,
            args.concurrency,
            args.incremental_backups,
            args.compress
        )

if __name__ == '__main__':
    main()
