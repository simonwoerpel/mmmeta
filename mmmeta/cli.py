import logging

import click

from mmmeta import mmmeta, settings
from mmmeta.logging import configure_logging

log = logging.getLogger(__name__)


@click.group()
@click.option(
    "--metadir",
    default=settings.MMMETA,
    help="Base path for reading meta info and storing state",
    show_default=True,
)
@click.option(
    "--files-root",
    default=settings.MMMETA_FILES_ROOT,
    help="Base path for actual files to generate metadir from",
    show_default=True,
)
@click.option(
    "--log-level",
    default=settings.LOGGING,
    help="Set logging level",
    show_default=True,
)
@click.pass_context
def cli(ctx, metadir, files_root, log_level, invoke_without_command=True):
    configure_logging(log_level)
    if not metadir:
        raise click.BadParameter("Missing metadir root")
    if ctx.obj is None:
        ctx.obj = {}
    ctx.obj["m"] = mmmeta(metadir)


@cli.command()
@click.option(
    "--replace",
    is_flag=True,
    default=False,
    help="Completly replace the meta database",
    show_default=True,
)
@click.option(
    "--ensure",
    is_flag=True,
    default=False,
    help="Ensure metadata files are present, soft-delete non-existing",
    show_default=True,
)
@click.option(
    "--ensure-files",
    is_flag=True,
    default=False,
    help="Ensure actual files are present (for local store only), soft-delete non-existing",  # noqa
    show_default=True,
)
@click.option(
    "--no-meta",
    is_flag=True,
    default=False,
    help="Read in actual files instead of json metadata files",
    show_default=True,
)
@click.pass_context
def generate(ctx, replace, ensure, ensure_files, no_meta):
    path = None  # FIXME
    ctx.obj["m"].generate(path, replace, ensure, ensure_files, no_meta)


@cli.command()
@click.option(
    "--replace",
    is_flag=True,
    default=False,
    help="Completly replace the local state database",
    show_default=True,
)
@click.option(
    "--cleanup",
    is_flag=True,
    default=False,
    help="Try to do some data migrations, can be helpful when things break.",
    show_default=True,
)
@click.pass_context
def update(ctx, replace, cleanup):
    ctx.obj["m"].update(replace, cleanup)


@cli.command()
@click.pass_context
def inspect(ctx):
    meta = ctx.obj["m"]
    for key, value in meta.inspect().items():
        click.echo(f"{key}: {value}")
    click.echo(f"{meta.store.to_string()}")


@cli.command()
@click.pass_context
def squash(ctx):
    ctx.obj["m"].squash()


@cli.command()
@click.pass_context
def dump(ctx):
    ctx.obj["m"].dump()
