"""Query a Tableau published datasource through a temporary workbook.

The batch account has no VizQL Data Service permission, so ``query-datasource``
returns 403.  Publishing a one-sheet workbook and exporting its view data is the
same mechanism the daily pipeline already uses for View 1/View 2, and it needs
only the permissions the pipeline already has.
"""

from __future__ import annotations

import os
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TABLEAU_NS = {"t": "http://tableau.com/api"}


def dimension_column(field: str, indent: str) -> str:
    return (
        f"{indent}<column aggregation='Count' datatype='string' default-type='nominal' "
        f"layered='true' name='[{field}]' pivot='key' role='dimension' type='nominal' "
        f"user-datatype='string' visual-totals='Default' />"
    )


def build_workbook(
    caption: str,
    content_url: str,
    ds_ref: str,
    sheet: str,
    dimensions: tuple[str, ...],
    extra_dependencies: str = "",
    extra_shelf: str = "",
    view_extras: str = "",
    cols_shelf: str = "",
) -> bytes:
    """Build a one-sheet workbook that lists ``dimensions`` from a published datasource.

    Measures belong on ``cols_shelf``.  Nesting two measures on the row shelf
    makes Tableau emit one exported row per measure, which silently doubles
    every aggregate.
    """
    if not dimensions:
        raise ValueError("At least one dimension is required")
    columns = "\n".join(dimension_column(field, "      ") for field in dimensions)
    dependencies = "\n".join(
        dimension_column(field, "            ")
        + f"\n            <column-instance column='[{field}]' derivation='None' "
          f"name='[none:{field}:nk]' pivot='key' type='nominal' />"
        for field in dimensions
    )
    if extra_dependencies:
        dependencies = f"{dependencies}\n{extra_dependencies}"
    shelf = extra_shelf or f"[{ds_ref}].[none:{dimensions[-1]}:nk]"
    remaining = dimensions if extra_shelf else dimensions[:-1]
    for field in reversed(remaining):
        shelf = f"([{ds_ref}].[none:{field}:nk] / {shelf})"
    return f"""<?xml version='1.0' encoding='utf-8' ?>
<workbook locale='ko_KR' original-version='18.1' source-build='2025.1.3 (20251.25.0611.0337)' source-platform='win' version='18.1' xml:base='https://tableau.ekmtc.com' xmlns:user='http://www.tableausoftware.com/xml/user'>
  <preferences />
  <datasources>
    <datasource caption='{caption}' inline='true' name='{ds_ref}' version='18.1'>
      <repository-location derived-from='http://localhost:8172/datasources/{content_url}?rev=' id='{content_url}' path='/datasources' revision='' />
      <connection channel='https' class='sqlproxy' dbname='{content_url}' directory='dataserver' port='443' server='tableau.ekmtc.com' server-ds-friendly-name='{caption}' server-oauth='' username='' workgroup-auth-mode='prompt'>
        <relation connection='sqlproxy' name='sqlproxy' table='[sqlproxy]' type='table' />
      </connection>
{columns}
    </datasource>
  </datasources>
  <worksheets>
    <worksheet name='{sheet}'>
      <table>
        <view>
          <datasources><datasource caption='{caption}' name='{ds_ref}' /></datasources>
          <datasource-dependencies datasource='{ds_ref}'>
{dependencies}
          </datasource-dependencies>
{view_extras}
          <aggregation value='true' />
        </view>
        <rows>{shelf}</rows>
        <cols>{cols_shelf}</cols>
      </table>
    </worksheet>
  </worksheets>
  <windows><window class='worksheet' name='{sheet}' /></windows>
</workbook>
""".encode("utf-8")


class SourceTableMissing(RuntimeError):
    """The datasource answered, but its underlying table no longer exists."""


def is_source_table_missing(error: BaseException) -> bool:
    """Recognize the error even when this module is imported under two names.

    ``scripts`` is on ``sys.path`` for the batch scripts and also imported as a
    package by the tests, which would otherwise produce two distinct classes.
    """
    return isinstance(error, SourceTableMissing) or type(error).__name__ == "SourceTableMissing"


def classify_export_error(detail: str) -> Exception:
    """Separate a missing source table from a permission problem."""
    text = detail or ""
    if "ORA-00942" in text or "does not exist" in text:
        table = ""
        marker = 'The table "'
        if marker in text:
            table = text.split(marker, 1)[1].split('"', 1)[0]
        return SourceTableMissing(
            f"Tableau datasource table is missing{f': {table}' if table else ''}"
        )
    if "403" in text or "unauthorized" in text.lower() or "권한" in text:
        return PermissionError(f"Tableau denied the published datasource query: {text[:400]}")
    return RuntimeError(f"Tableau view export failed: {text[:400]}")


def publish_and_export(
    twb: bytes,
    workbook_name: str,
    save_path: Path,
    timeout_seconds: int = 1800,
) -> int:
    """Publish ``twb``, export its first view as CSV, then delete the workbook."""
    import daily_3w_dashboard as dashboard

    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    session, api_ver, site_id = dashboard.tableau_rest_api()
    base = f"{dashboard.TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}"
    workbook_id = None
    try:
        response = session.get(
            f"{base}/workbooks",
            params={"filter": f"name:eq:{workbook_name}"},
            headers={"Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        for stale in response.json().get("workbooks", {}).get("workbook", []):
            session.delete(f"{base}/workbooks/{stale['id']}", timeout=60)

        boundary = "----TableauBoundary"
        payload = (
            f'--{boundary}\r\nContent-Disposition: name="request_payload"\r\n'
            f'Content-Type: text/xml\r\n\r\n'
            f'<tsRequest><workbook name="{workbook_name}" showTabs="true">'
            f'<project id="{dashboard.TEMP_WB_PROJECT_ID}"/></workbook></tsRequest>\r\n'
            f'--{boundary}\r\nContent-Disposition: name="tableau_workbook"; '
            f'filename="{workbook_name}.twb"\r\nContent-Type: application/xml\r\n\r\n'
        ).encode("utf-8") + twb + f"\r\n--{boundary}--\r\n".encode("utf-8")
        response = session.post(
            f"{base}/workbooks",
            params={"overwrite": "true", "skipConnectionCheck": "true"},
            data=payload,
            headers={"Content-Type": f"multipart/mixed; boundary={boundary}"},
            timeout=600,
        )
        if response.status_code not in (200, 201):
            detail = (response.text or "").strip().replace("\n", " ")[:500]
            raise RuntimeError(
                f"Temporary workbook publish failed: HTTP {response.status_code}: {detail}"
            )
        element = ET.fromstring(response.content).find(".//t:workbook", TABLEAU_NS)
        if element is None:
            raise RuntimeError("Temporary workbook publish returned no workbook element")
        workbook_id = element.get("id")

        view_id = None
        for _ in range(6):
            views = session.get(
                f"{base}/workbooks/{workbook_id}/views",
                headers={"Accept": "application/json"},
                timeout=60,
            )
            if views.ok:
                found = views.json().get("views", {}).get("view", [])
                if found:
                    view_id = found[0]["id"]
                    break
            time.sleep(8)
        if not view_id:
            raise RuntimeError("Temporary workbook published but no view became available")

        temp_path = save_path.with_name(save_path.name + ".download")
        temp_path.unlink(missing_ok=True)
        written = 0
        with session.get(
            f"{base}/views/{view_id}/data",
            params={"includeAllColumns": "true"},
            stream=True,
            timeout=(30, timeout_seconds),
        ) as export:
            if not export.ok:
                raise classify_export_error((export.text or "").strip().replace("\n", " "))
            with temp_path.open("wb") as handle:
                for chunk in export.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
                        written += len(chunk)
        if written <= 0:
            temp_path.unlink(missing_ok=True)
            raise RuntimeError("Tableau view export returned an empty response")
        os.replace(temp_path, save_path)
        return written
    finally:
        try:
            if workbook_id:
                session.delete(f"{base}/workbooks/{workbook_id}", timeout=60)
        finally:
            try:
                session.post(
                    f"{dashboard.TABLEAU_SERVER}/api/{api_ver}/auth/signout", timeout=10
                )
            except Exception:
                pass
            session.close()
