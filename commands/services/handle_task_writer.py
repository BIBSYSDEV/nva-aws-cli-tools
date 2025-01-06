class HandleTaskWriterService:
    def __init__(self):
        self.sikt_prefix = '//hdl.handle.net/11250'
        self.sikt_prefix_test = '//hdl.handle.net/11250.1'
        pass

    def _is_sikt_handle(self, handle):
        return self.sikt_prefix in handle or self.sikt_prefix_test in handle

    def _get_sikt_additional_identifier_handle(self, publication):
        for additionalIdentifier in publication.get('additionalIdentifiers', []):
            if additionalIdentifier.get('type') == 'HandleIdentifier' or \
               (additionalIdentifier.get('source') == 'handle' and additionalIdentifier.get('type') == 'AdditionalIdentifier'):
                handle = additionalIdentifier.get('value')
                if self._is_sikt_handle(handle):
                    return handle
        return None

    def process_item(self, publication):
        task = {}
        additional_identifier_handle = self._get_sikt_additional_identifier_handle(publication)
        top_handle = publication.get('publication')
        task['identifiter'] = publication['identifier']
        task['publication'] = publication

        if top_handle and self._is_sikt_handle(top_handle):
            task['action'] = "nop" # all good, already sikt managed handle in place
        elif top_handle and not self._is_sikt_handle(top_handle):
            if additional_identifier_handle:
                task['action'] = "move_top_to_additional_and_promote_additional" 
                task['updated_handle'] = additional_identifier_handle
            else:
                task['action'] = "move_top_to_additional_and_create_new_top"
        elif not top_handle and additional_identifier_handle:
            task['action'] = "promote_additional"
            task['updated_handle'] = additional_identifier_handle
        elif not top_handle and not additional_identifier_handle:
            task['action'] = "create_new_top"
        return task