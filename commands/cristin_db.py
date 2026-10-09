import functools
import logging
from collections.abc import Collection

import click
from rich.console import Console
from rich.table import Table

from commands.services.cristin_db import (
    DEFAULT_DB_USER,
    CristinDatabaseError,
    CristinDatabaseService,
    MergeResult,
    PersonProfile,
)
from commands.utils import AppContext

logger = logging.getLogger(__name__)

LOSING_PERSON_LABEL = "Forsvinner"
KEEPING_PERSON_LABEL = "Beholdes"
NAME_ROW_LABEL = "Navn"


def handle_database_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except CristinDatabaseError as error:
            raise click.ClickException(str(error))

    return wrapper


def credential_options(func):
    func = click.option(
        "--db-user",
        "username",
        default=None,
        help=f"Database user to connect as, looked up in the Vault secret. Defaults to {DEFAULT_DB_USER}.",
    )(func)
    return click.option(
        "--vault-path",
        default=None,
        help="Vault path holding the Cristin database credentials. Defaults to the one matching the AWS profile.",
    )(func)


@click.group(name="cristin-db")
@click.pass_obj
def cristin_db(ctx: AppContext) -> None:
    """Run manual Cristin database routines directly against Oracle.

    Requires Tailscale (groups RG_Tailscale_Cristin / -prod) and access to the
    database credentials in Vault (group RG_VAULT_Cristin). The AWS profile
    decides the environment: a profile containing "prod" uses CRISPRD,
    everything else uses CRISTST.
    """


@cristin_db.command(name="merge-person")
@click.argument("from_lopenr", type=int)
@click.argument("to_lopenr", type=int)
@click.option(
    "--yes", is_flag=True, default=False, help="Skip the confirmation prompt."
)
@credential_options
@click.pass_obj
@handle_database_errors
def merge_person(
    ctx: AppContext,
    from_lopenr: int,
    to_lopenr: int,
    yes: bool,
    vault_path: str | None,
    username: str | None,
) -> None:
    """Merge Cristin person FROM_LOPENR into TO_LOPENR (PK_FDS200010.P_Merge_Person).

    FROM_LOPENR is the profile that disappears, TO_LOPENR the one that is kept.
    Move the publications in NVA first with `manual-update contributor-identifier`,
    or run the whole routine with `routines merge-person`.
    """
    run_merge_person(ctx, from_lopenr, to_lopenr, yes, vault_path, username)


def run_merge_person(
    ctx: AppContext,
    from_lopenr: int,
    to_lopenr: int,
    yes: bool,
    vault_path: str | None,
    username: str | None = None,
    console: Console | None = None,
) -> None:
    console = console or Console()
    with CristinDatabaseService(
        ctx.profile, vault_path=vault_path, username=username
    ) as service:
        print_merge_preview(console, service, from_lopenr, to_lopenr)
        if not yes:
            click.confirm(
                f"Slå sammen {from_lopenr} inn i {to_lopenr} i {service.environment}?",
                default=True,
                abort=True,
            )
        console.print(
            f"[bold]Slår sammen {from_lopenr} inn i {to_lopenr} i Cristin "
            f"{service.environment}[/bold]"
        )
        result = service.merge_person(from_lopenr, to_lopenr)
        print_merge_result(console, result, "UTFØRT")


def require_persons_exist(
    ctx: AppContext,
    lopenrs: Collection[int],
    vault_path: str | None,
    username: str | None = None,
    console: Console | None = None,
) -> None:
    console = console or Console()
    with CristinDatabaseService(
        ctx.profile, vault_path=vault_path, username=username
    ) as service:
        for lopenr in lopenrs:
            person = require_person(service, lopenr)
            console.print(f"{lopenr}: {person.full_name} ({service.environment})")


def print_merge_preview(
    console: Console,
    service: CristinDatabaseService,
    from_lopenr: int,
    to_lopenr: int,
) -> None:
    losing = require_person(service, from_lopenr)
    keeping = require_person(service, to_lopenr)
    table = Table(title=f"Cristin {service.environment} – {service.dsn}")
    table.add_column("Felt")
    table.add_column(f"{LOSING_PERSON_LABEL} ({from_lopenr})")
    table.add_column(f"{KEEPING_PERSON_LABEL} ({to_lopenr})")
    table.add_row(NAME_ROW_LABEL, losing.full_name, keeping.full_name)
    for attribute in sorted(set(losing.attributes) | set(keeping.attributes)):
        table.add_row(
            attribute,
            as_text(losing.attributes.get(attribute)),
            as_text(keeping.attributes.get(attribute)),
        )
    for label in sorted(set(losing.related_counts) | set(keeping.related_counts)):
        table.add_row(
            label,
            as_text(losing.related_counts.get(label)),
            as_text(keeping.related_counts.get(label)),
        )
    console.print(table)


def require_person(service: CristinDatabaseService, lopenr: int) -> PersonProfile:
    person = service.fetch_person(lopenr)
    if person is None:
        raise CristinDatabaseError(f"Fant ingen person med PERSONLOPENR {lopenr}")
    return person


def print_merge_result(console: Console, result: MergeResult, phase: str) -> None:
    console.print(f"[bold]{phase}[/bold] – sessionid: {result.session_id}")
    for line in result.output_lines:
        console.print(f"  {line}")


def as_text(value) -> str:
    return "" if value is None else str(value)
