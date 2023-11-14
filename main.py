from __future__ import annotations

import argparse
import dataclasses
import datetime
import gzip
import logging
import os
import shlex
import shutil
import subprocess
import sys
from argparse import Namespace
from pathlib import Path
from subprocess import CalledProcessError
from typing import Optional

from dotenv import load_dotenv
from rocketry import Rocketry

from mysqlbackup.s3 import S3Config, Uploader

LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logging.basicConfig(format=LOGGING_FORMAT)

CONFIG_BACKUP_DIR = ""

BACKUP_FILE_NAME_DATE_FORMAT = "%d%m%y"
BACKUP_FILE_NAME_PATTERN = "{db_name}_{date}.sql"
BACKUP_COMMAND = "mysqldump"


class MysqlBackupError(Exception):
    pass


@dataclasses.dataclass
class DbConfig:
    host: str
    name: str
    user: str
    password: str
    port: int = 3306


@dataclasses.dataclass
class BackupConfig:
    dir: Path
    schedule: Optional[str]
    compression_level: int = 9


@dataclasses.dataclass
class Config:
    db: DbConfig
    backup: BackupConfig
    s3: S3Config
    # notification: NotificationConfig

    @staticmethod
    def from_args(args: Namespace) -> Config:
        try:
            # email_config = EmailNotificationConfig(
            #     server=EmailServerConfig(
            #         host=args.email_host,
            #         port=args.email_port,
            #         ssl=EmailSSL.value_of(args.email_ssl),
            #         auth_required=args.email_auth,
            #         credentials=EmailCredentialsConfig(
            #             username=args.email_username,
            #             password=args.email_password
            #         ) if args.email_auth else None
            #     ),
            #     to_address=args.email_to,
            #     from_address=args.email_from,
            # ) if args.notification else None

            return Config(
                db=DbConfig(
                    host=args.db_host,
                    name=args.db_name,
                    password="root",
                    port=args.db_port,
                    user=args.db_user
                ),
                backup=BackupConfig(
                    compression_level=args.backup_compression_level,
                    dir=Path(args.backup_dir),
                    schedule=args.schedule
                ),
                s3=S3Config(
                    endpoint=args.s3_endpoint,
                    region=args.s3_region,
                    access_key=args.s3_access_key,
                    secret_key=args.s3_secret_key,
                    bucket=args.s3_bucket
                )
                # notification=NotificationConfig(
                #     enabled=args.notification,
                #     email=email_config
                # )
            )
        except AttributeError as e:
            raise MysqlBackupError(
                "Unable to create config from arguments") from e


def build_backup_command(config: Config) -> str:
    return (f"{BACKUP_COMMAND} -h {config.db.host} -P {config.db.port} "
            f"-p -u {config.db.user} --databases {config.db.name}")


def parse_arguments() -> Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--db-host", help="MySQL host",
                        required=os.getenv("DB_HOST") is None,
                        default=os.getenv("DB_HOST"),
                        metavar="HOST")
    parser.add_argument("--logging-level", help="Logging level",
                        default="INFO", metavar="LEVEL")
    parser.add_argument("--db-port", help="MySQL port",
                        type=int,
                        default=os.getenv("DB_PORT", 3306),
                        metavar="PORT")
    parser.add_argument("--db-user", help="MySQL user",
                        required=os.getenv("DB_USER") is None,
                        default=os.getenv("DB_USER"),
                        metavar="USER")
    parser.add_argument("--backup-compression-level",
                        help="Backup file compression level (1-9)",
                        choices=range(1, 10), type=int, metavar="LVL",
                        default=os.getenv("BACKUP_COMPRESSION_LEVEL", 9))
    parser.add_argument("--backup-dir", help="Backup directory",
                        default=os.getenv("BACKUP_DIR"), metavar="DIR")
    parser.add_argument("--s3-endpoint", help="S3 endpoint",
                        required=os.getenv("S3_ENDPOINT") is None,
                        default=os.getenv("S3_ENDPOINT"),
                        metavar="ENDPOINT")
    parser.add_argument("--s3-access-key", help="S3 access key",
                        required=os.getenv("S3_ACCESS_KEY") is None,
                        default=os.getenv("S3_ACCESS_KEY"),
                        metavar="ACCESS_KEY")
    parser.add_argument("--s3-secret-key", help="S3 secret key",
                        required=os.getenv("S3_SECRET_KEY") is None,
                        default=os.getenv("S3_SECRET_KEY"),
                        metavar="SECRET_KEY")
    parser.add_argument("--s3-region", help="S3 region",
                        required=os.getenv("S3_REGION") is None,
                        default=os.getenv("S3_REGION"),
                        metavar="REGION")
    parser.add_argument("--s3-bucket", help="S3 bucket",
                        required=os.getenv("S3_BUCKET") is None,
                        default=os.getenv("S3_BUCKET"),
                        metavar="BUCKET")

    subparsers = parser.add_subparsers(dest="command")

    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("db_name", help="MySQL database name")

    schedule_parser = subparsers.add_parser("schedule")
    schedule_parser.add_argument("db_name", help="MySQL database name")
    schedule_parser.add_argument("schedule", help="Backup creation schedule")

    return parser.parse_args()


def compress_file(path: Path, mode: str = "b",
                  compression_level: int = 9) -> Path:
    compressed_file_path: Path = path.parent / (path.name + '.gz')

    try:
        with open(path, "r" + mode) as file:
            with gzip.open(
                    compressed_file_path, "w" + mode,
                    compresslevel=compression_level) as compressed_file:
                shutil.copyfileobj(file, compressed_file)

        return compressed_file_path
    except Exception as e:
        compressed_file_path.unlink(missing_ok=True)
        raise MysqlBackupError("Unable to compress backup file") from e


def create_compressed_backup(config: Config) -> Path:
    backup_path = config.backup.dir.joinpath(
        BACKUP_FILE_NAME_PATTERN.format(db_name=config.db.name,
                                        date=datetime.datetime.now().strftime(
                                            BACKUP_FILE_NAME_DATE_FORMAT)))

    try:
        create_backup(backup_path, config)
        return compress_file(backup_path, mode="t",
                             compression_level=config.backup.compression_level)
    finally:
        backup_path.unlink(missing_ok=True)


def create_backup(backup_path: Path, config: Config):
    try:
        command = shlex.split(build_backup_command(config))

        with open(backup_path, "w") as backup_file:
            subprocess.run(command, stdout=backup_file,
                           input=config.db.password.encode(), check=True)
    except CalledProcessError as e:
        logger.error(f"Something went wrong while making backup:\n{e.stderr}")
        raise MysqlBackupError("Unable to make a backup") from e


def create_scheduled(config: Config):
    backup_path: Optional[Path] = None

    try:
        backup_path = create_compressed_backup(config)
        Uploader(config.s3).upload(
            backup_path, f"{config.db.name}/{backup_path.name}")
    finally:
        if backup_path is not None:
            backup_path.unlink()


def create(config: Config):
    backup_path: Optional[Path] = None
    try:
        backup_path = create_compressed_backup(config)
        Uploader(config.s3).upload(
            backup_path, f"{config.db.name}/{backup_path.name}")
    except (Exception,):
        logger.exception("Something went wrong")
        sys.exit(1)
    finally:
        if backup_path is not None:
            backup_path.unlink()


if __name__ == '__main__':
    load_dotenv()
    arguments: Namespace = parse_arguments()

    logging.basicConfig(level=arguments.logging_level)

    config = Config.from_args(arguments)
    logger.debug(f"Running with config: {config}")

    try:
        if arguments.command == "create":
            create(config)
        else:
            scheduler = Rocketry()
            scheduler.task(start_cond=config.backup.schedule,
                           name="Backup creation",
                           func=create_scheduled,
                           parameters=dict(config=config))
            scheduler.run()
    except KeyboardInterrupt:
        sys.exit(0)
