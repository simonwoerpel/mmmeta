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
