SIKT_HANDLE_PREFIX = "//hdl.handle.net/11250"
SIKT_HANDLE_PREFIX_TEST = "//hdl.handle.net/11250.1"


def build_task(publication: dict, prefix: str) -> dict:
    additional_identifier_handle = _find_sikt_additional_identifier_handle(publication)
    top_handle = publication.get("publication")

    task = {
        "identifier": publication.get("identifier"),
        "publication": publication,
        "handles_to_import": _find_additional_identifier_handles(publication, prefix),
    }

    if top_handle and _is_sikt_handle(top_handle):
        task["action"] = "nop"
    elif top_handle and not _is_sikt_handle(top_handle):
        if additional_identifier_handle:
            task["action"] = "move_top_to_additional_and_promote_additional"
            task["top_handle"] = additional_identifier_handle
        else:
            task["action"] = "move_top_to_additional_and_create_new_top"
    elif not top_handle and additional_identifier_handle:
        task["action"] = "promote_additional"
        task["top_handle"] = additional_identifier_handle
    else:
        task["action"] = "create_new_top"

    return task


def _is_sikt_handle(handle: str) -> bool:
    return SIKT_HANDLE_PREFIX in handle or SIKT_HANDLE_PREFIX_TEST in handle


def _find_sikt_additional_identifier_handle(publication: dict) -> str | None:
    for additional_identifier in publication.get("additionalIdentifiers", []):
        if _is_handle_identifier(additional_identifier):
            handle = additional_identifier.get("value")
            if _is_sikt_handle(handle):
                return handle
    return None


def _find_additional_identifier_handles(publication: dict, prefix: str) -> list[str]:
    handles = []
    for additional_identifier in publication.get("additionalIdentifiers", []):
        if _is_handle_identifier(additional_identifier):
            handle = additional_identifier.get("value")
            if f"//hdl.handle.net/{prefix}" in handle:
                handles.append(handle)
    return handles


def _is_handle_identifier(additional_identifier: dict) -> bool:
    return additional_identifier.get("type") == "HandleIdentifier" or (
        additional_identifier.get("source") == "handle"
        and additional_identifier.get("type") == "AdditionalIdentifier"
    )
