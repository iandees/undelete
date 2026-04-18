import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from daemon.watcher import Watcher


SAMPLE_ADIFF = b"""<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <action type="delete">
    <old>
      <node id="12345" version="2" lon="1.0" lat="2.0">
        <tag k="name" v="Deleted Node"/>
      </node>
    </old>
    <new>
      <node id="12345" version="3" timestamp="2025-01-14T14:51:43Z"
            uid="100" user="deleter" changeset="999"
            lat="2.0" lon="1.0" visible="false"/>
    </new>
  </action>
</osm>"""


def test_get_latest_sequence():
    mock_response = MagicMock()
    mock_response.text = (
        "#Sat Jan 14 14:51:37 UTC 2025\n"
        "sequenceNumber=6429815\n"
        "timestamp=2025-01-14T14\\:50\\:58Z\n"
    )
    mock_response.raise_for_status = MagicMock()
    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=Path("/tmp/test"))
        seq = watcher.get_latest_sequence()
        assert seq == 6429815


def test_save_and_load_state(tmp_path):
    watcher = Watcher(data_dir=tmp_path)
    watcher.save_state(6429815)
    assert watcher.load_state() == 6429815


def test_fetch_and_process(tmp_path):
    mock_response = MagicMock()
    mock_response.content = SAMPLE_ADIFF
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=tmp_path)
        count = watcher.fetch_and_process(6429815)
        assert count == 1

    # Check that a daily file was written
    geojsonl_files = list((tmp_path / "deletions").glob("*.geojsonl"))
    assert len(geojsonl_files) == 1
    line = geojsonl_files[0].read_text().strip()
    feature = json.loads(line)
    assert feature["properties"]["osm_id"] == 12345
    assert feature["properties"]["tags"]["name"] == "Deleted Node"


def test_fetch_404_returns_zero(tmp_path):
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=tmp_path)
        count = watcher.fetch_and_process(9999999999)
        assert count == 0
