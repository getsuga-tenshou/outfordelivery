from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient

from dashboard import app as dashboard

client = TestClient(dashboard.app)


@pytest.fixture
def map_env(tmp_path, monkeypatch):
    monkeypatch.setattr(dashboard, "REPO", tmp_path)
    monkeypatch.delenv("CARTO_MAP_API_KEY", raising=False)
    return tmp_path / ".env"


def test_map_style_uses_env_file_key_without_loading_other_settings(map_env, monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    map_env.write_text('CARTO_MAP_API_KEY="file-key&+?"\nAWS_ACCESS_KEY_ID=unrelated-secret\n')
    response = client.get("/api/map/style")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    style = response.json()
    base = style["sources"]["base"]
    url = urlsplit(base["tiles"][0])
    assert url.hostname == "a.basemaps.cartocdn.com"
    assert url.path == "/light_all/{z}/{x}/{y}.png"
    assert parse_qs(url.query) == {"key": ["file-key&+?"]}
    assert "OpenStreetMap" in base["attribution"] and "CARTO" in base["attribution"]
    assert "unrelated-secret" not in response.text
    assert "AWS_ACCESS_KEY_ID" not in dashboard.os.environ


def test_shell_key_overrides_env_file(map_env, monkeypatch):
    map_env.write_text("CARTO_MAP_API_KEY=file-key\n")
    monkeypatch.setenv("CARTO_MAP_API_KEY", "shell-key")
    style = client.get("/api/map/style").json()
    url = urlsplit(style["sources"]["base"]["tiles"][0])
    assert parse_qs(url.query) == {"key": ["shell-key"]}


@pytest.mark.parametrize("contents", [None, "CARTO_MAP_API_KEY=\n", "CARTO_MAP_API_KEY='   '\n"])
def test_missing_key_keeps_a_style_for_parcel_overlay(map_env, contents):
    if contents is not None:
        map_env.write_text(contents)
    response = client.get("/api/map/style")
    assert response.status_code == 200
    style = response.json()
    assert style["version"] == 8
    assert style["sources"] == {}
    assert style["layers"][0]["type"] == "background"


def test_empty_shell_key_disables_file_key(map_env, monkeypatch):
    map_env.write_text("CARTO_MAP_API_KEY=file-key\n")
    monkeypatch.setenv("CARTO_MAP_API_KEY", "")
    assert client.get("/api/map/style").json()["sources"] == {}
