import re
import polars as pl
from datetime import datetime
from typing import Optional
from commands.services.users_api import UsersAndRolesService
from commands.services.customers_api import build_customer_lookup
from commands.services.models import User, ExportResult


class UserExportService:
    def __init__(self, profile: Optional[str]):
        self.profile = profile
        self.users_service = UsersAndRolesService(profile)
        self.customer_lookup: Optional[dict[str, str]] = None

    def export_to_excel(
        self,
        output_filename: Optional[str] = None,
        exclude_only_roles: Optional[list[str]] = None,
        include_roles: Optional[list[str]] = None
    ) -> ExportResult:
        all_users = self.users_service.get_all_users()
        self.customer_lookup = build_customer_lookup(self.profile)

        excluded_only_roles_set = set(exclude_only_roles) if exclude_only_roles else set()
        included_roles_set = set(include_roles) if include_roles else set()

        filtered_users = self._filter_users(
            all_users,
            excluded_only_roles_set,
            included_roles_set
        )

        if not output_filename:
            profile_name = self.profile or "default"
            datetime_str = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            output_filename = f"users-{profile_name}-{datetime_str}.xlsx"

        self._create_excel_file(filtered_users, output_filename)

        return ExportResult(
            total_users=len(all_users),
            exported_users=len(filtered_users),
            filename=output_filename
        )

    def _filter_users(
        self,
        all_users: list[User],
        excluded_only_roles_set: set[str],
        included_roles_set: set[str]
    ) -> list[User]:
        filtered_users = []
        for user in all_users:
            user_role_names = {role.name for role in user.roles if role.name}

            if excluded_only_roles_set:
                if not (user_role_names and user_role_names.issubset(excluded_only_roles_set)):
                    filtered_users.append(user)
            elif included_roles_set:
                if user_role_names.intersection(included_roles_set):
                    filtered_users.append(user)
            else:
                filtered_users.append(user)

        return filtered_users

    def _create_excel_file(self, users: list[User], output_filename: str) -> None:
        headers = [
            "Username",
            "Cristin ID",
            "Given Name",
            "Family Name",
            "Affiliation",
            "Institution UUID",
            "Institution Name",
            "Roles",
            "Access Rights",
            "Viewing Scope - Included Units"
        ]
        rows = [self._build_user_row(user) for user in users]
        df = pl.DataFrame(rows, schema=headers, orient="row")

        df.write_excel(output_filename, worksheet="Users and Roles", autofit=True)

    def _build_user_row(self, user: User) -> list[str]:
        institution_uuid = ""
        institution_name = ""
        if user.institution:
            uuid_match = re.search(r"(?<=customer/).+", user.institution)
            if uuid_match:
                institution_uuid = uuid_match.group()
                institution_name = self.customer_lookup.get(institution_uuid, "Unknown")

        role_names = [role.name for role in user.roles if role.name]
        access_rights = set()
        for role in user.roles:
            access_rights.update(role.access_rights)

        included_units_list = []
        if user.viewing_scope and user.viewing_scope.included_units:
            included_units_list = [str(unit) for unit in user.viewing_scope.included_units]

        return [
            user.username or "",
            user.cristin_id or "",
            user.given_name or "",
            user.family_name or "",
            user.affiliation or "",
            institution_uuid,
            institution_name,
            ", ".join(role_names),
            ", ".join(sorted(access_rights)),
            ", ".join(included_units_list)
        ]
