from dataclasses import dataclass, field
from typing import Optional, Any


@dataclass
class Role:
    name: str
    access_rights: list[str] = field(default_factory=list)
    type: str = "ROLE"
    primary_key_hash_key: Optional[str] = None
    primary_key_range_key: Optional[str] = None

    @classmethod
    def from_dynamodb(cls, item: dict[str, Any]) -> "Role":
        access_rights = item.get("accessRights", [])
        if isinstance(access_rights, set):
            access_rights = list(access_rights)
        return cls(
            name=item.get("name", ""),
            access_rights=access_rights,
            type=item.get("type", "ROLE"),
            primary_key_hash_key=item.get("PrimaryKeyHashKey"),
            primary_key_range_key=item.get("PrimaryKeyRangeKey"),
        )


@dataclass
class ViewingScope:
    included_units: list[str] = field(default_factory=list)
    excluded_units: Optional[list[str]] = None
    type: str = "ViewingScope"

    @classmethod
    def from_dynamodb(cls, item: dict[str, Any]) -> "ViewingScope":
        included_units = item.get("includedUnits", [])
        if isinstance(included_units, set):
            included_units = list(included_units)

        excluded_units = item.get("excludedUnits")
        if isinstance(excluded_units, set):
            excluded_units = list(excluded_units)

        return cls(
            included_units=included_units,
            excluded_units=excluded_units,
            type=item.get("type", "ViewingScope"),
        )


@dataclass
class User:
    username: str
    cristin_id: Optional[str] = None
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    affiliation: Optional[str] = None
    institution: Optional[str] = None
    institution_cristin_id: Optional[str] = None
    roles: list[Role] = field(default_factory=list)
    viewing_scope: Optional[ViewingScope] = None
    type: str = "USER"
    primary_key_hash_key: Optional[str] = None
    primary_key_range_key: Optional[str] = None
    secondary_index1_hash_key: Optional[str] = None
    secondary_index1_range_key: Optional[str] = None
    secondary_index2_hash_key: Optional[str] = None
    secondary_index2_range_key: Optional[str] = None

    @classmethod
    def from_dynamodb(cls, item: dict[str, Any]) -> "User":
        roles = [Role.from_dynamodb(role_item) for role_item in item.get("roles", [])]

        viewing_scope_data = item.get("viewingScope")
        viewing_scope = (
            ViewingScope.from_dynamodb(viewing_scope_data)
            if viewing_scope_data
            else None
        )

        return cls(
            username=item.get("username", ""),
            cristin_id=item.get("cristinId"),
            given_name=item.get("givenName"),
            family_name=item.get("familyName"),
            affiliation=item.get("affiliation"),
            institution=item.get("institution"),
            institution_cristin_id=item.get("institutionCristinId"),
            roles=roles,
            viewing_scope=viewing_scope,
            type=item.get("type", "USER"),
            primary_key_hash_key=item.get("PrimaryKeyHashKey"),
            primary_key_range_key=item.get("PrimaryKeyRangeKey"),
            secondary_index1_hash_key=item.get("SecondaryIndex1HashKey"),
            secondary_index1_range_key=item.get("SecondaryIndex1RangeKey"),
            secondary_index2_hash_key=item.get("SecondaryIndex2HashKey"),
            secondary_index2_range_key=item.get("SecondaryIndex2RangeKey"),
        )

    @dataclass
    class ExcelRow:
        username: str
        cristin_id: str
        given_name: str
        family_name: str
        affiliation: str
        institution_uuid: str
        institution_name: str
        roles: str
        access_rights: str
        viewing_scope_included_units: str

        @classmethod
        def headers(cls) -> list[str]:
            return [
                "Username",
                "Cristin ID",
                "Given Name",
                "Family Name",
                "Affiliation",
                "Institution UUID",
                "Institution Name",
                "Roles",
                "Access Rights",
                "Viewing Scope - Included Units",
            ]

        def to_list(self) -> list[str]:
            return [
                self.username,
                self.cristin_id,
                self.given_name,
                self.family_name,
                self.affiliation,
                self.institution_uuid,
                self.institution_name,
                self.roles,
                self.access_rights,
                self.viewing_scope_included_units,
            ]

    def to_excel_row(self, institution_name_lookup: dict[str, str]) -> ExcelRow:
        import re

        institution_uuid = ""
        institution_name = ""
        if self.institution:
            uuid_match = re.search(r"(?<=customer/).+", self.institution)
            if uuid_match:
                institution_uuid = uuid_match.group()
                institution_name = institution_name_lookup.get(
                    institution_uuid, "Unknown"
                )

        role_names = [role.name for role in self.roles if role.name]
        access_rights = set()
        for role in self.roles:
            access_rights.update(role.access_rights)

        included_units_list = []
        if self.viewing_scope and self.viewing_scope.included_units:
            included_units_list = [
                str(unit) for unit in self.viewing_scope.included_units
            ]

        return User.ExcelRow(
            username=self.username or "",
            cristin_id=self.cristin_id or "",
            given_name=self.given_name or "",
            family_name=self.family_name or "",
            affiliation=self.affiliation or "",
            institution_uuid=institution_uuid,
            institution_name=institution_name,
            roles=", ".join(role_names),
            access_rights=", ".join(sorted(access_rights)),
            viewing_scope_included_units=", ".join(included_units_list),
        )


@dataclass
class Customer:
    identifier: str
    name: str
    cristin_id: Optional[str] = None
    display_name: Optional[str] = None
    short_name: Optional[str] = None
    archiveName: Optional[str] = None
    cname: Optional[str] = None
    feideOrganizationDomain: Optional[str] = None
    customer_of: Optional[str] = None

    @classmethod
    def from_dynamodb(cls, item: dict[str, Any]) -> "Customer":
        return cls(
            identifier=item.get("identifier", ""),
            name=item.get("name", "Unknown"),
            cristin_id=item.get("cristinId"),
            display_name=item.get("displayName"),
            short_name=item.get("shortName"),
            archiveName=item.get("archiveName"),
            cname=item.get("cname"),
            feideOrganizationDomain=item.get("feideOrganizationDomain"),
            customer_of=item.get("customerOf"),
        )


@dataclass
class ExportResult:
    total_users: int
    exported_users: int
    filename: str
