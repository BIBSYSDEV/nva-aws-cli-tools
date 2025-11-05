class HandleTaskWriterService:
    NVA_SIKT_SOURCE_NAME = "nva@sikt"

    def __init__(self, application_domain, controlled_prefixes):
        self.controlled_prefixes = controlled_prefixes
        self.application_domain = application_domain

    def _is_controlled_handle(self, handle):
        if not handle:
            return False
        return any(
            f"//hdl.handle.net/{prefix}" in handle
            for prefix in self.controlled_prefixes
        )

    def _get_all_handles(self, publication):
        handles = []

        top_handle = publication.get("handle")
        if top_handle:
            handles.append(
                {"value": top_handle, "source_name": None, "location": "top"}
            )

        for additional_identifier in publication.get("additionalIdentifiers", []):
            if additional_identifier.get("type") == "HandleIdentifier":
                handle_value = additional_identifier.get("value")
                source_name = additional_identifier.get("sourceName")
                if handle_value:
                    handles.append(
                        {
                            "value": handle_value,
                            "source_name": source_name,
                            "location": "additional",
                        }
                    )

        return handles

    def process_item(self, publication):
        all_handles = self._get_all_handles(publication)

        handles_to_import = [
            handle
            for handle in all_handles
            if self._is_controlled_handle(handle["value"])
            and handle["source_name"] != self.NVA_SIKT_SOURCE_NAME
        ]

        if not handles_to_import:
            return []

        tasks = []
        for handle in handles_to_import:
            task = {
                "identifier": publication.get("identifier"),
                "publication_uri": self._get_landing_page_uri(
                    publication.get("identifier")
                ),
                "handle": handle["value"]
            }
            tasks.append(task)

        return tasks

    def process_handle_from_json(self, handle_value, handle_info):
        full_handle_url = f"https://hdl.handle.net/{handle_value}"

        if not self._is_controlled_handle(full_handle_url):
            return []

        source_names = handle_info.get("sourceName", [])
        if self.NVA_SIKT_SOURCE_NAME in source_names:
            return []

        nva_ids = handle_info.get("nvaIds", [])
        if not nva_ids:
            return []

        if len(nva_ids) != 1:
            return []

        tasks = []

        for nva_id in nva_ids:
            task = {
                "identifier": nva_id,
                "publication_uri": self._get_landing_page_uri(nva_id),
                "handle": full_handle_url
            }
            tasks.append(task)

        return tasks

    def _get_landing_page_uri(self, publicationIdentifier):
        return f"https://{self.application_domain}/registration/{publicationIdentifier}"
