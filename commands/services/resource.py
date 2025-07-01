import copy


class Resource:
    def __init__(self, data: dict):
        self.data = data
        pass

    def identifier(self) -> str:
        return self.data.get("identifier", None)

    def migrate_contributor_affiliations(
        self, old_identifier: str, new_identifier: str
    ):
        updated = copy.deepcopy(self.data)
        contributors = updated.get("entityDescription", {}).get("contributors", [])
        for contributor in contributors:
            affiliations = contributor.get("affiliations", [])
            for affiliation in affiliations:
                aff_id = affiliation.get("id")
                if aff_id and aff_id.endswith(old_identifier):
                    print(
                        f"Updating contributor.affiliation.id {aff_id} → ...{new_identifier}"
                    )
                    affiliation["id"] = aff_id[: -len(old_identifier)] + new_identifier
        self.data = updated

    def migrate_owner_affiliation(self, old_identifier: str, new_identifier: str):
        updated = copy.deepcopy(self.data)
        owner_affiliation = updated.get("resourceOwner", {}).get(
            "ownerAffiliation", None
        )

        if owner_affiliation and owner_affiliation.endswith(old_identifier):
            print(
                f"Updating resourceOwner.ownerAffiliation {owner_affiliation} → ...{new_identifier}"
            )
            updated.get("resourceOwner", {})["ownerAffiliation"] = (
                owner_affiliation[: -len(old_identifier)] + new_identifier
            )
        self.data = updated

    def data(self) -> dict:
        return self.data
