import click
import logging

from commands.utils import AppContext
from commands.services.s3_versions import (
    download_versions,
    build_git_history,
    find_bucket,
)

logger = logging.getLogger(__name__)

DEFAULT_BUCKET_SUBSTRING = "persisted-resources"
DEFAULT_PREFIX = "resources/"


@click.group()
@click.pass_obj
def s3(ctx: AppContext):
    """S3 utilities."""


@s3.command()
@click.argument("object_key")
@click.option(
    "--bucket",
    default=DEFAULT_BUCKET_SUBSTRING,
    show_default=True,
    help="Substring of the bucket name to match",
)
@click.option(
    "--prefix",
    default=DEFAULT_PREFIX,
    show_default=True,
    help="Key prefix prepended to OBJECT_KEY",
)
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
    object_key: str,
    bucket: str,
    prefix: str,
    output_dir: str,
    no_git: bool,
) -> None:
    """Download all versions of an S3 object and create a git history.

    Resolves OBJECT_KEY to s3://<bucket matching BUCKET>/<PREFIX><OBJECT_KEY>.
    Useful for the common case of inspecting expanded publications.

    Example:
      s3 get-versions 0198cc877130-63254c68-0000-0000-0000-000000000000.gz
    """
    s3_client = ctx.session.client("s3")

    try:
        resolved_bucket = find_bucket(s3_client, bucket)
    except ValueError as exc:
        raise click.ClickException(str(exc))

    key = prefix.rstrip("/") + "/" + object_key.lstrip("/")
    _fetch_and_build(s3_client, resolved_bucket, key, output_dir, no_git)


@s3.command()
@click.argument("s3_uri")
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
def get_versions_uri(
    ctx: AppContext,
    s3_uri: str,
    output_dir: str,
    no_git: bool,
) -> None:
    """Download all versions using a full S3 URI.

    S3_URI accepts s3://bucket/key or bucket/key.

    Example:
      s3 get-versions-uri persisted-resources-755923822223/resources/0198cc877130-....gz
      s3 get-versions-uri s3://persisted-resources-755923822223/resources/0198cc877130-....gz
    """
    uri = s3_uri.removeprefix("s3://")
    bucket, _, object_path = uri.partition("/")
    if not object_path:
        raise click.BadParameter(
            "S3_URI must be bucket/key or s3://bucket/key", param_hint="S3_URI"
        )

    s3_client = ctx.session.client("s3")
    _fetch_and_build(s3_client, bucket, object_path, output_dir, no_git)


def _fetch_and_build(
    s3_client, bucket: str, key: str, output_dir: str, no_git: bool
) -> None:
    try:
        version_dir = download_versions(s3_client, bucket, key, output_dir)
    except ValueError as exc:
        raise click.ClickException(str(exc))

    click.echo(f"Versions saved to: {version_dir}")

    if not no_git:
        try:
            build_git_history(version_dir, key)
        except RuntimeError as exc:
            raise click.ClickException(f"Git error: {exc}")
        click.echo(f"Git history created in: {version_dir}")
