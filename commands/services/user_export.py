import re
import polars as pl
from datetime import datetime
from commands.services.users_api import UsersAndRolesService
from commands.services.customers_api import build_customer_lookup


class UserExportService:
    def __init__(self, profile):
        self.profile = profile
        self.users_service = UsersAndRolesService(profile)
        self.customer_lookup = None

    def export_to_excel(
        self,
        output_filename=None,
        exclude_only_roles=None,
        include_roles=None
    ):
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

        return {
            "total_users": len(all_users),
            "exported_users": len(filtered_users),
            "filename": output_filename
        }

    def _filter_users(self, all_users, excluded_only_roles_set, included_roles_set):
        filtered_users = []
        for user in all_users:
            roles_list = user.get("roles", [])
            user_role_names = {role.get("name", "") for role in roles_list if role.get("name")}

            if excluded_only_roles_set:
                if not (user_role_names and user_role_names.issubset(excluded_only_roles_set)):
                    filtered_users.append(user)
            elif included_roles_set:
                if user_role_names.intersection(included_roles_set):
                    filtered_users.append(user)
            else:
                filtered_users.append(user)

        return filtered_users

    def _create_excel_file(self, users, output_filename):
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

    def _build_user_row(self, user):
        username = user.get("username", "")
        cristin_id = user.get("cristinId", "")
        given_name = user.get("givenName", "")
        family_name = user.get("familyName", "")
        affiliation = user.get("affiliation", "")
        institution = user.get("institution", "")

        institution_uuid = ""
        institution_name = ""
        if institution:
            uuid_match = re.search(r"(?<=customer/).+", institution)
            if uuid_match:
                institution_uuid = uuid_match.group()
                institution_name = self.customer_lookup.get(institution_uuid, "Unknown")

        roles_list = user.get("roles", [])
        role_names = []
        access_rights = set()

        for role in roles_list:
            role_name = role.get("name", "")
            if role_name:
                role_names.append(role_name)

            role_access_rights = role.get("accessRights", [])
            access_rights.update(role_access_rights)

        viewing_scope = user.get("viewingScope", {})
        included_units = viewing_scope.get("includedUnits", [])
        if included_units is None:
            included_units = []

        included_units_list = []
        if isinstance(included_units, (list, set)):
            included_units_list = [str(unit) for unit in included_units]
        elif included_units:
            included_units_list = [str(included_units)]

        return [
            username,
            cristin_id,
            given_name,
            family_name,
            affiliation,
            institution_uuid,
            institution_name,
            ", ".join(role_names),
            ", ".join(sorted(access_rights)),
            ", ".join(included_units_list)
        ]
