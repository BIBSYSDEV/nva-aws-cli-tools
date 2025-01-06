import click
import json

from commands.services.users_api import UsersAndRolesService

@click.group()
def users():
    pass

@users.command(help="Search users by user values")
@click.option('--profile', envvar='AWS_PROFILE', default='default', help='The AWS profile to use.')
@click.argument('search_term', required=True, nargs=-1)
def search(profile, search_term):
    search_term = ' '.join(search_term)
    result = UsersAndRolesService(profile).search(search_term)
    click.echo(json.dumps(result, indent=2, sort_keys=True, default=str, ensure_ascii=False))