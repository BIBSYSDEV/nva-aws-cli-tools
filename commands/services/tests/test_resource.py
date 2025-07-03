from commands.services.resource import Resource


def test_migrate_contributor_affiliations():
    data = {
        "entityDescription": {
            "contributors": [
                {"affiliations": [{"id": "https://somehost/10.1.0.0"}]},
                {"affiliations": [{"id": "https://somehost/10.1.0.0"}]},
            ]
        }
    }

    resource = Resource(data)

    resource.migrate_contributor_affiliations("10.1.0.0", "10.2.0.0")

    assert (
        resource.data.get("entityDescription")
        .get("contributors")[0]
        .get("affiliations")[0]
        .get("id")
        == "https://somehost/10.2.0.0"
    )
    assert (
        resource.data.get("entityDescription")
        .get("contributors")[1]
        .get("affiliations")[0]
        .get("id")
        == "https://somehost/10.2.0.0"
    )


def test_migrate_owner_affiliation():
    data = {"resourceOwner": {"ownerAffiliation": "https://somehost/10.1.0.0"}}

    resource = Resource(data)

    resource.migrate_owner_affiliation("10.1.0.0", "10.2.0.0")

    assert (
        resource.data.get("resourceOwner").get("ownerAffiliation")
        == "https://somehost/10.2.0.0"
    )
