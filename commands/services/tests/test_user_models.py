from commands.services.user_models import Role, User, ViewingScope


def test_viewing_scope_from_dynamodb_handles_null_included_units():
    item = {"includedUnits": None, "excludedUnits": None, "type": "ViewingScope"}

    viewing_scope = ViewingScope.from_dynamodb(item)

    assert viewing_scope.included_units == []
    assert viewing_scope.excluded_units is None


def test_user_from_dynamodb_handles_viewing_scope_with_null_included_units():
    item = {
        "username": "user@example.org",
        "viewingScope": {"includedUnits": None, "type": "ViewingScope"},
    }

    user = User.from_dynamodb(item)

    assert user.viewing_scope is not None
    assert user.viewing_scope.included_units == []


def test_role_from_dynamodb_handles_null_access_rights():
    item = {"name": "Editor", "accessRights": None, "type": "ROLE"}

    role = Role.from_dynamodb(item)

    assert role.access_rights == []


def test_user_from_dynamodb_handles_null_roles():
    item = {"username": "user@example.org", "roles": None}

    user = User.from_dynamodb(item)

    assert user.roles == []
