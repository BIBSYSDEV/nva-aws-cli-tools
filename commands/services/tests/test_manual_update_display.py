from commands.manual_update import _describe_change, _labelled


def test_describe_change_single_line():
    assert (
        _describe_change("Ada Lovelace", "123", "Grace Hopper", "456")
        == "Ada Lovelace (123) → Grace Hopper (456)"
    )


def test_describe_change_without_labels():
    assert _describe_change(None, "123", None, "456") == "123 → 456"


def test_describe_change_multiline_labels_render_as_block():
    old_label = "Ada Lovelace, fnr 01019012345\n  Overingeniør, Institutt for IT"
    new_label = "Grace Hopper"

    assert _describe_change(old_label, "123", new_label, "456") == (
        "Ada Lovelace, fnr 01019012345 (123)\n"
        "  Overingeniør, Institutt for IT\n"
        "  ↓\n"
        "Grace Hopper (456)"
    )


def test_labelled_appends_value_to_first_line():
    assert _labelled("Name\n  employment", "123") == "Name (123)\n  employment"
