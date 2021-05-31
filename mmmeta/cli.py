import click
import logging

from mmmeta import settings, mmmeta


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
@click.pass_context
def cli(ctx, metadir, files_root, invoke_without_command=True):
    logging.basicConfig(level=logging.INFO)
    if not metadir:
        raise click.BadParameter("Missing metadir root")
    if ctx.obj is None:
        ctx.obj = {}
    ctx.obj["m"] = mmmeta(metadir)


@cli.command()
@click.pass_context
def generate(ctx):
    ctx.obj["m"].generate()


@cli.command()
@click.pass_context
def update(ctx):
    ctx.obj["m"].update()


@cli.command()
@click.pass_context
def inspect(ctx):
    meta = ctx.obj["m"]
    for key, value in meta.inspect().items():
        click.echo(f"{key}: {value}")
    click.echo(f"{meta.store.to_string()}")
