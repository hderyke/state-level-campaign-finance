"""Arizona: where a candidate's election year comes from, the committee
details file repairing its own missing header, and the per-cycle registry
fetch refusing a short page. No network. Run: python3 tests/test_arizona.py"""
import csv
import sys
import tempfile
from pathlib import Path
from _harness import run, patched
from src.pipeline.parsers import arizona as parser
from src.pipeline.scrapers import arizona as scraper

CYCLE_COLS = ["cycle", "entity_id", "committee_name", "office_name", "party_name", "income", "expense"]


def _cycles(rows):
    d = Path(tempfile.mkdtemp())
    with open(d / "az_committee_cycles.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CYCLE_COLS)
        for cycle, eid, office in rows:
            w.writerow([cycle, eid, "Some Committee", office, "Democratic", "0", "0"])
    return d


def test_each_office_gets_its_own_most_recent_cycle():
    # One committee, two offices over the years (the Katie Hobbs shape).
    d = _cycles([("2018", "201800057", "Secretary of State"),
                 ("2022", "201800057", "Secretary of State"),
                 ("2022", "201800057", "Governor"),
                 ("2026", "201800057", "Governor")])
    with patched(parser, "RAW_DIR", d):
        got = parser.load_office_cycles()
    assert got == {("201800057", "Secretary of State"): "2022",
                   ("201800057", "Governor"): "2026"}


def test_file_order_does_not_matter():
    d = _cycles([("2026", "1", "Governor"), ("2014", "1", "Governor"), ("2022", "1", "Governor")])
    with patched(parser, "RAW_DIR", d):
        assert parser.load_office_cycles() == {("1", "Governor"): "2026"}


def test_missing_file_leaves_years_blank():
    with patched(parser, "RAW_DIR", Path(tempfile.mkdtemp())):
        assert parser.load_office_cycles() == {}


def test_bad_rows_are_skipped():
    d = _cycles([("", "1", "Governor"), ("n/a", "1", "Governor"), ("2026", "", "Governor"),
                 ("2026", "2", "Governor")])
    with patched(parser, "RAW_DIR", d):
        assert parser.load_office_cycles() == {("2", "Governor"): "2026"}


def _detail_row(eid):
    return [eid] + ["x"] * (len(scraper.DETAIL_COLS) - 1)


def test_headerless_details_file_is_repaired():
    out = Path(tempfile.mkdtemp()) / "az_committee_details.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([_detail_row("100"), _detail_row("200")])
    scraper._ensure_detail_header(out)
    with open(out, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert [r["entity_id"] for r in rows] == ["100", "200"]      # nothing lost, nothing eaten


def test_good_details_file_is_left_alone():
    out = Path(tempfile.mkdtemp()) / "az_committee_details.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([scraper.DETAIL_COLS, _detail_row("100")])
    before = out.read_bytes()
    scraper._ensure_detail_header(out)
    scraper._ensure_detail_header(out)
    assert out.read_bytes() == before


def test_empty_or_absent_details_file_is_fine():
    d = Path(tempfile.mkdtemp())
    scraper._ensure_detail_header(d / "nope.csv")
    (d / "empty.csv").write_text("")
    scraper._ensure_detail_header(d / "empty.csv")
    assert (d / "empty.csv").read_text() == ""


class _Resp:
    def __init__(self, payload): self._p = payload
    def raise_for_status(self): pass
    def json(self): return self._p


class _Session:
    def __init__(self, payload): self.payload, self.sent = payload, None
    def post(self, url, **kw):
        self.sent = (url, kw)
        return _Resp(self.payload)


def test_cycle_fetch_asks_for_less_active_committees_too():
    s = _Session({"data": [{}] * 3, "recordsTotal": 3})
    assert len(scraper.fetch_registry_cycle(s, 2025, 2026)) == 3
    url, kw = s.sent
    assert url.endswith("/GetNEWTableData/")
    assert kw["params"]["IsLessActive"] == "true"
    assert (kw["params"]["startYear"], kw["params"]["endYear"]) == ("2025", "2026")


def test_cycle_fetch_refuses_a_short_page():
    try:
        scraper.fetch_registry_cycle(_Session({"data": [{}] * 100, "recordsTotal": 717}), 2025, 2026)
    except ValueError as e:
        assert "100 of 717" in str(e)
    else:
        raise AssertionError("a short page was accepted")


def test_cycle_fetch_accepts_a_few_more_rows_than_reported():
    # 2022 really does this: 511 reported, 513 returned.
    assert len(scraper.fetch_registry_cycle(_Session({"data": [{}] * 513, "recordsTotal": 511}), 2021, 2022)) == 513


def test_cycle_fetch_refuses_an_unexpected_response():
    for payload in ([], {"data": None}, {"error": "x"}):
        try:
            scraper.fetch_registry_cycle(_Session(payload), 2025, 2026)
        except ValueError:
            continue
        raise AssertionError(f"accepted {payload!r}")


if __name__ == "__main__":
    sys.exit(run(globals()))
