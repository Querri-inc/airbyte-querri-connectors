"""Querri Data API client — standalone copy for destination connector.

This is identical to source_querri/client.py. It exists so the destination
connector can be installed independently without requiring the source package.
"""

import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("airbyte")

DEFAULT_BASE_URL = "https://app.querri.com/api/v1"
DEFAULT_PAGE_SIZE = 10_000
REQUEST_TIMEOUT = 60


class QuerriClient:
    """HTTP client for the Querri Data API."""

    def __init__(self, api_key: str, org_id: str, base_url: str = DEFAULT_BASE_URL):
        self.api_key = api_key
        self.org_id = org_id
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "X-Tenant-ID": org_id,
            "Content-Type": "application/json",
        })

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _raise_for_status(self, resp: requests.Response):
        if resp.ok:
            return
        try:
            body = resp.json()
            error = body.get("error") or body.get("detail", {}).get("error", {})
            msg = error.get("message", resp.text)
        except Exception:
            msg = resp.text
        raise requests.HTTPError(
            f"HTTP {resp.status_code}: {msg}",
            response=resp,
        )

    def check_connection(self) -> bool:
        resp = self.session.get(self._url("/data/sources"), timeout=REQUEST_TIMEOUT)
        self._raise_for_status(resp)
        return True

    def list_sources(self) -> List[Dict[str, Any]]:
        resp = self.session.get(self._url("/data/sources"), timeout=REQUEST_TIMEOUT)
        self._raise_for_status(resp)
        return resp.json().get("data", [])

    def get_source(self, source_id: str) -> Dict[str, Any]:
        resp = self.session.get(
            self._url(f"/data/sources/{source_id}"), timeout=REQUEST_TIMEOUT
        )
        self._raise_for_status(resp)
        return resp.json()

    def read_source_data(
        self, source_id: str, page: int = 1, page_size: int = DEFAULT_PAGE_SIZE
    ) -> Dict[str, Any]:
        resp = self.session.get(
            self._url(f"/data/sources/{source_id}/data"),
            params={"page": page, "page_size": page_size},
            timeout=REQUEST_TIMEOUT,
        )
        self._raise_for_status(resp)
        result = resp.json()
        # Normalize: API may return total_count or total_rows
        if "total_rows" not in result and "total_count" in result:
            result["total_rows"] = result["total_count"]
        return result

    def create_source(self, name: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        resp = self.session.post(
            self._url("/data/sources"),
            json={"name": name, "rows": rows},
            timeout=REQUEST_TIMEOUT,
        )
        self._raise_for_status(resp)
        return resp.json()

    def append_rows(self, source_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        resp = self.session.post(
            self._url(f"/data/sources/{source_id}/rows"),
            json={"rows": rows},
            timeout=REQUEST_TIMEOUT,
        )
        self._raise_for_status(resp)
        return resp.json()

    def replace_data(self, source_id: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        resp = self.session.put(
            self._url(f"/data/sources/{source_id}/data"),
            json={"rows": rows},
            timeout=REQUEST_TIMEOUT,
        )
        self._raise_for_status(resp)
        return resp.json()

    def delete_source(self, source_id: str) -> Dict[str, Any]:
        resp = self.session.delete(
            self._url(f"/data/sources/{source_id}"), timeout=REQUEST_TIMEOUT
        )
        self._raise_for_status(resp)
        return resp.json()

    def find_source_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        sources = self.list_sources()
        for source in sources:
            if source.get("name") == name:
                return source
        return None
