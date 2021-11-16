import json
import logging
from typing import Any, List, Optional

import click

from datahub.cli.cli_utils import get_entity

logger = logging.getLogger(__name__)


@click.command(
    name="get",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option("--urn", required=False, type=str)
@click.option("-a", "--aspect", required=False, multiple=True, type=str)
@click.pass_context
def get(ctx: Any, urn: Optional[str], aspect: List[str]) -> None:
    if urn is None:
        urn = ctx.args[0]
        logger.debug(f"Using urn from args {urn}")
    click.echo(json.dumps(get_entity(urn=urn, aspect=aspect), sort_keys=True, indent=2))
