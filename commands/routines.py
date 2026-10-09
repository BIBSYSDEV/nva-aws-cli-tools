import logging

import click
from rich.console import Console

from commands.cristin_db import (
    handle_database_errors,
    require_persons_exist,
    run_merge_person,
    vault_path_option,
)
from commands.manual_update import contributor_identifier, has_pending_results
from commands.utils import AppContext

logger = logging.getLogger(__name__)

VALIDATION_HEADING = "Sjekker at begge personprofilene finnes i Cristin"
NVA_STEP_HEADING = "Steg 1 – flytt forskningsresultater i NVA"
CRISTIN_STEP_HEADING = "Steg 2 – slå sammen personprofilene i Cristin"
PENDING_RESULTS_MESSAGE = (
    "Steg 1 stoppet på limitReached med flere treff igjen i NVA. "
    "Profilene er ikke slått sammen i Cristin. Kjør manual-update contributor-identifier "
    "med høyere --limit til ingen treff gjenstår, og kjør deretter denne kommandoen på nytt."
)


@click.group()
@click.pass_obj
def routines(ctx: AppContext) -> None:
    """Run the manual routines that span both NVA and Cristin.

    These commands only orchestrate the per-system commands, in the order the
    manual for manual Cristin changes describes them.
    """


@routines.command(name="merge-person")
@click.argument("from_lopenr", type=int)
@click.argument("to_lopenr", type=int)
@click.option(
    "--limit",
    type=click.IntRange(min=1),
    default=None,
    help="Max number of NVA resources to move in the first step.",
)
@click.option(
    "--yes", is_flag=True, default=False, help="Skip the confirmation prompts."
)
@vault_path_option
@click.pass_context
@handle_database_errors
def merge_person(
    click_context: click.Context,
    from_lopenr: int,
    to_lopenr: int,
    limit: int | None,
    yes: bool,
    vault_path: str | None,
) -> None:
    """Merge Cristin person FROM_LOPENR into TO_LOPENR, in NVA and then in Cristin.

    FROM_LOPENR is the profile that disappears, TO_LOPENR the one that is kept.
    Step 1 moves the publications with the ManuallyUpdatePublications Lambda,
    step 2 runs PK_FDS200010.P_Merge_Person in the Cristin database.
    Both steps ask for confirmation before they write anything.
    """
    app_context: AppContext = click_context.obj
    console = Console()

    console.rule(VALIDATION_HEADING)
    require_persons_exist(app_context, (from_lopenr, to_lopenr), vault_path, console)

    console.rule(NVA_STEP_HEADING)
    report = click_context.invoke(
        contributor_identifier,
        old_value=str(from_lopenr),
        new_value=str(to_lopenr),
        limit=limit,
        yes=yes,
    )
    if has_pending_results(report):
        raise click.ClickException(PENDING_RESULTS_MESSAGE)

    console.rule(CRISTIN_STEP_HEADING)
    run_merge_person(
        app_context,
        from_lopenr,
        to_lopenr,
        yes,
        vault_path,
        console=console,
    )
