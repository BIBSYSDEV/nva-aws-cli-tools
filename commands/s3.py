import click
import boto3
import logging

from commands.utils import AppContext
from commands.services.s3_versions import download_versions, build_git_history

logger = logging.getLogger(__name__)


@click.group()
@click.pass_obj
def s3(ctx: AppContext):
    """S3 utilities."""
    pass


@s3.command(
    help="Download all versions of an S3 object and create a git history.\n\n"
    "S3_PATH is bucket/key, e.g. 'persisted-resources-755923822223/resources/0198cc877130-....gz'."
)
@click.argument("s3_path")
@click.option(
    "-o",
    "--output-dir",
    default=".",
    show_default=True,
    help="Base directory where the version folder is created",
)
@click.option(
    "--no-git",
    is_flag=True,
    help="Skip git history creation",
)
@click.pass_obj
def get_versions(
    ctx: AppContext,
    s3_path: str,
    output_dir: str,
    no_git: bool,
) -> None:
    bucket, _, object_path = s3_path.partition("/")
    if not object_path:
        raise click.BadParameter("S3_PATH must be bucket/key", param_hint="S3_PATH")

    session = boto3.Session(profile_name=ctx.profile)
    s3_client = session.client("s3")

    version_dir = download_versions(s3_client, bucket, object_path, output_dir)
    click.echo(f"Versions saved to: {version_dir}")

    if not no_git:
        build_git_history(version_dir)
        click.echo(f"Git history created in: {version_dir}")
