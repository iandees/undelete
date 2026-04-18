import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from pipeline.merge_upload import R2Uploader


def test_upload_file(tmp_path):
    test_file = tmp_path / "test.pmtiles"
    test_file.write_bytes(b"fake pmtiles data")

    mock_client = MagicMock()
    with patch("pipeline.merge_upload.boto3.client", return_value=mock_client):
        uploader = R2Uploader(
            endpoint_url="https://test.r2.cloudflarestorage.com",
            access_key_id="key",
            secret_access_key="secret",
            bucket_name="test-bucket",
        )
        uploader.upload_file(test_file, "test.pmtiles")

    mock_client.upload_file.assert_called_once_with(
        str(test_file), "test-bucket", "test.pmtiles"
    )


def test_upload_today_geojson(tmp_path):
    geojson_file = tmp_path / "today.geojson"
    geojson_file.write_text('{"type":"FeatureCollection","features":[]}')

    mock_client = MagicMock()
    with patch("pipeline.merge_upload.boto3.client", return_value=mock_client):
        uploader = R2Uploader(
            endpoint_url="https://test.r2.cloudflarestorage.com",
            access_key_id="key",
            secret_access_key="secret",
            bucket_name="test-bucket",
        )
        uploader.upload_today_geojson(geojson_file, feature_count=0)

    mock_client.upload_file.assert_called_with(
        str(geojson_file),
        "test-bucket",
        "today.geojson",
        ExtraArgs={"ContentType": "application/geo+json"},
    )
