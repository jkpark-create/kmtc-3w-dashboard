import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "daily_3w_dashboard_publish_test",
    ROOT / "daily_3w_dashboard.py",
)
dashboard = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = dashboard
SPEC.loader.exec_module(dashboard)


WORKBOOK_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<workbook>
  <filter class="quantitative" column="Calculation_0356804709482497">
    <min>#2026-01-01 00:00:00#</min>
    <max>#2026-01-31 00:00:00#</max>
  </filter>
</workbook>
"""


class FakeResponse:
    def __init__(self, *, status_code=200, content=b"", text="", json_data=None):
        self.status_code = status_code
        self.content = content
        self.text = text
        self._json_data = json_data or {}

    def json(self):
        return self._json_data


class FakeSession:
    def __init__(self, publish_response):
        self.publish_response = publish_response
        self.post_calls = []

    def get(self, url, **kwargs):
        if url.endswith("/workbooks"):
            return FakeResponse(json_data={"workbooks": {"workbook": []}})
        if url.endswith(f"/workbooks/{dashboard.BKG_WB_ID}/content"):
            return FakeResponse(content=WORKBOOK_XML)
        raise AssertionError(f"Unexpected GET: {url}")

    def post(self, url, **kwargs):
        self.post_calls.append((url, kwargs))
        return self.publish_response


class TableauPublishTest(unittest.TestCase):
    def test_publish_skips_connection_check_and_uses_returned_content_url(self):
        response = FakeResponse(
            status_code=201,
            content=(
                b'<tsResponse xmlns="http://tableau.com/api">'
                b'<workbook contentUrl="published-temp"><project id="p"/></workbook>'
                b'</tsResponse>'
            ),
        )
        session = FakeSession(response)

        result = dashboard.ensure_temp_workbook(
            session,
            "3.29",
            "site-id",
            start="2026-01-01 00:00:00",
            end="2026-01-31 00:00:00",
            workbook_name="temp-test",
        )

        self.assertEqual(result, "published-temp")
        self.assertEqual(len(session.post_calls), 1)
        _, kwargs = session.post_calls[0]
        self.assertEqual(
            kwargs["params"],
            {"overwrite": "true", "skipConnectionCheck": "true"},
        )

    def test_publish_http_error_is_raised_without_discovery_polling(self):
        session = FakeSession(
            FakeResponse(status_code=403, text="permission denied\nfor workbook")
        )

        with mock.patch.object(dashboard.time, "sleep") as sleep:
            with self.assertRaisesRegex(
                RuntimeError,
                r"Tableau workbook publish failed: HTTP 403: permission denied for workbook",
            ):
                dashboard.ensure_temp_workbook(
                    session,
                    "3.29",
                    "site-id",
                    start="2026-01-01 00:00:00",
                    end="2026-01-31 00:00:00",
                    workbook_name="temp-test",
                )

        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
