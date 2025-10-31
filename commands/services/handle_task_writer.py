import boto3

class HandleTaskWriterService:
    NVA_SIKT_SOURCE_NAME = "nva@sikt"

    def __init__(self, profile, controlled_prefixes=None):
        if controlled_prefixes is None:
            controlled_prefixes = ["11250", "11250.1"]
        self.controlled_prefixes = controlled_prefixes
        self.session = boto3.Session(profile_name=profile)
        self.ssm = self.session.client("ssm")
        self.application_domain = self._get_system_parameter("/NVA/ApplicationDomain")

        
    def _is_controlled_handle(self, handle):
        if not handle:
            return False
        return any(f"//hdl.handle.net/{prefix}" in handle for prefix in self.controlled_prefixes)
    
    def _get_all_handles(self, publication):
        handles = []

        top_handle = publication.get("handle")
        if top_handle:
            handles.append({
                "value": top_handle,
                "source_name": None,
                "location": "top"
            })

        for additional_identifier in publication.get("additionalIdentifiers", []):
            if additional_identifier.get("type") == "HandleIdentifier":
                handle_value = additional_identifier.get("value")
                source_name = additional_identifier.get("sourceName")
                if handle_value:
                    handles.append({
                        "value": handle_value,
                        "source_name": source_name,
                        "location": "additional"
                    })

        return handles

    def process_item(self, publication):
        all_handles = self._get_all_handles(publication)

        handles_to_import = [
            handle for handle in all_handles
            if self._is_controlled_handle(handle["value"])
            and handle["source_name"] != self.NVA_SIKT_SOURCE_NAME
        ]

        if not handles_to_import:
            return []

        tasks = []
        for handle in handles_to_import:
            task = {
                "identifier": publication.get("identifier"),
                "publication_uri": self._get_landing_page_uri(publication.get("identifier")),
                "handle": handle["value"]
            }
            tasks.append(task)

        return tasks
    
    def _get_landing_page_uri(self, publicationIdentifier):
        return f"https://{self.application_domain}/registration/{publicationIdentifier}"
    
    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]