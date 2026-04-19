import json
from io import BytesIO
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
  <action type="modify">
    <old>
      <node id="67890" version="1" lon="3.0" lat="4.0">
        <tag k="name" v="Old Name"/>
      </node>
    </old>
    <new>
      <node id="67890" version="2" timestamp="2025-01-14T15:00:00Z"
            uid="200" user="editor" changeset="1000"
            lon="3.1" lat="4.1">
        <tag k="name" v="New Name"/>
      </node>
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
    mock_response.raw = BytesIO(SAMPLE_ADIFF)
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=tmp_path)
        count = watcher.fetch_and_process(6429815)
        assert count == 2

    # Check that a daily file was written
    geojsonl_files = list((tmp_path / "deletions").glob("*.geojsonl"))
    assert len(geojsonl_files) == 1
    lines = geojsonl_files[0].read_text().strip().split("\n")
    assert len(lines) == 2

    features = [json.loads(line) for line in lines]
    actions = {f["properties"]["action"] for f in features}
    assert "delete" in actions
    assert "modify" in actions

    delete_feat = [f for f in features if f["properties"]["action"] == "delete"][0]
    assert delete_feat["properties"]["osm_id"] == 12345
    assert delete_feat["properties"]["tags"] == {}
    assert delete_feat["properties"]["old_tags"]["name"] == "Deleted Node"

    modify_feat = [f for f in features if f["properties"]["action"] == "modify"][0]
    assert modify_feat["properties"]["osm_id"] == 67890
    assert modify_feat["properties"]["tags"]["name"] == "New Name"


def test_fetch_404_returns_none(tmp_path):
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=tmp_path)
        result = watcher.fetch_and_process(9999999999)
        assert result is None
