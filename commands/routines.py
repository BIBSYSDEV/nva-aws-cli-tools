import logging

import click
from rich.console import Console

from commands.cristin_db import (
    handle_database_errors,
    run_merge_person,
    vault_path_option,
)
from commands.manual_update import contributor_identifier
from commands.utils import AppContext

logger = logging.getLogger(__name__)

NVA_STEP_HEADING = "Steg 1 – flytt forskningsresultater i NVA"
CRISTIN_STEP_HEADING = "Steg 2 – slå sammen personprofilene i Cristin"


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

    console.rule(NVA_STEP_HEADING)
    click_context.invoke(
        contributor_identifier,
        old_value=str(from_lopenr),
        new_value=str(to_lopenr),
        search_pairs=(),
        limit=limit,
        page_size=None,
        yes=yes,
        dry_run_only=False,
        no_dry_run=False,
    )

    console.rule(CRISTIN_STEP_HEADING)
    run_merge_person(
        app_context,
        from_lopenr,
        to_lopenr,
        yes,
        vault_path,
        console=console,
    )
