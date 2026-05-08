from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from cli import cli
from commands.services.user_models import Role, User


def make_user(username: str, role_names: list[str]) -> User:
    return User(username=username, roles=[Role(name=name) for name in role_names])


@patch("commands.users.UsersAndRolesService")
def test_role_summary_counts_roles_correctly(mock_service_class: MagicMock) -> None:
    mock_service_class.return_value.get_all_users.return_value = [
        make_user("alice", ["Creator", "Editor"]),
        make_user("bob", ["Creator"]),
        make_user("carol", ["Editor"]),
    ]

    result = CliRunner().invoke(
        cli, ["--quiet", "users", "role-summary", "--csv-output"]
    )

    assert result.exit_code == 0, result.exception
    assert "Creator,2" in result.output
    assert "Editor,2" in result.output


@patch("commands.users.UsersAndRolesService")
def test_role_summary_filters_by_role(mock_service_class: MagicMock) -> None:
    mock_service_class.return_value.get_all_users.return_value = [
        make_user("alice", ["Creator", "Editor"]),
        make_user("bob", ["Creator"]),
    ]

    result = CliRunner().invoke(
        cli, ["--quiet", "users", "role-summary", "--csv-output", "--roles", "Creator"]
    )

    assert result.exit_code == 0, result.exception
    assert "Creator,2" in result.output
    assert "Editor" not in result.output


@patch("commands.users.UsersAndRolesService")
def test_role_summary_sorts_by_count_descending(mock_service_class: MagicMock) -> None:
    mock_service_class.return_value.get_all_users.return_value = [
        make_user("alice", ["Editor"]),
        make_user("bob", ["Creator"]),
        make_user("carol", ["Creator"]),
        make_user("dave", ["Creator"]),
    ]

    result = CliRunner().invoke(
        cli, ["--quiet", "users", "role-summary", "--csv-output"]
    )

    assert result.exit_code == 0, result.exception
    lines = [
        line
        for line in result.output.splitlines()
        if "," in line and line != "Role,Number of users"
    ]
    assert lines[0].startswith("Creator")
    assert lines[1].startswith("Editor")


@patch("commands.users.ExternalUserService")
def test_create_external_passes_shortname_to_service(
    mock_service_class: MagicMock,
) -> None:
    mock_external_user = MagicMock()
    mock_external_user.client_data = {"clientId": "id", "clientSecret": "secret"}
    mock_service_class.return_value.create.return_value = mock_external_user

    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "users",
            "create-external",
            "--customer",
            "bb3d0c0c-5065-4623-9b98-5810983c2478",
            "--intended_purpose",
            "test-integration",
            "--scopes",
            "https://api.nva.unit.no/scopes/third-party/publication-read",
            "--shortname",
            "MyOrg",
        ],
    )

    assert result.exit_code == 0, result.exception
    mock_service_class.return_value.create.assert_called_once_with(
        "bb3d0c0c-5065-4623-9b98-5810983c2478",
        "test-integration",
        ["https://api.nva.unit.no/scopes/third-party/publication-read"],
        "MyOrg",
    )


@patch("commands.users.ExternalUserService")
def test_create_external_passes_none_shortname_when_not_provided(
    mock_service_class: MagicMock,
) -> None:
    mock_external_user = MagicMock()
    mock_external_user.client_data = {"clientId": "id", "clientSecret": "secret"}
    mock_service_class.return_value.create.return_value = mock_external_user

    result = CliRunner().invoke(
        cli,
        [
            "--quiet",
            "users",
            "create-external",
            "--customer",
            "bb3d0c0c-5065-4623-9b98-5810983c2478",
            "--intended_purpose",
            "test-integration",
            "--scopes",
            "https://api.nva.unit.no/scopes/third-party/publication-read",
        ],
    )

    assert result.exit_code == 0, result.exception
    mock_service_class.return_value.create.assert_called_once_with(
        "bb3d0c0c-5065-4623-9b98-5810983c2478",
        "test-integration",
        ["https://api.nva.unit.no/scopes/third-party/publication-read"],
        None,
    )
