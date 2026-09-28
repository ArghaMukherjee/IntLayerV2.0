import re

from ilcommon.templating import has_placeholders, render


def test_whole_string_placeholder_keeps_its_type():
    out = render({"q": "{{rand_int:1:3}}", "p": "{{rand_float:5:6}}", "n": "{{seq}}"}, {"seq": 7})
    assert isinstance(out["q"], int) and 1 <= out["q"] <= 3
    assert isinstance(out["p"], float) and 5 <= out["p"] <= 6
    assert out["n"] == 7


def test_embedded_placeholders_become_text():
    out = render({"id": "ORD-{{seq}}-{{short_id}}", "c": "{{choice:EUR|USD}}", "d": "{{date}}"}, {"seq": 3})
    assert re.fullmatch(r"ORD-3-[0-9A-F]{6}", out["id"])
    assert out["c"] in ("EUR", "USD")
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", out["d"])


def test_nested_structures_and_unknown_placeholders():
    template = {"items": [{"sku": "S-{{rand_int:1:9}}"}], "keep": "{{nope}}", "n": 5, "b": True}
    out = render(template)
    assert re.fullmatch(r"S-\d", out["items"][0]["sku"])
    assert out["keep"] == "{{nope}}" and out["n"] == 5 and out["b"] is True
    assert has_placeholders(template) and not has_placeholders({"a": [1, "x"]})
