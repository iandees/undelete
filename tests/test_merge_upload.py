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




def _uploader(pages):
    mock_client = MagicMock()
    mock_client.get_paginator.return_value.paginate.return_value = pages
    with patch("pipeline.merge_upload.boto3.client", return_value=mock_client):
        return R2Uploader("https://x", "k", "s", "bucket"), mock_client


def test_list_dates_returns_published_dates_across_pages():
    pages = [
        {"Contents": [{"Key": "osm-changes/date=2026-04-19/data.parquet"},
                      {"Key": "osm-changes/metadata.json"}]},
        {"Contents": [{"Key": "osm-changes/date=2026-04-18/data.parquet"},
                      {"Key": "osm-changes/date=2026-04-20/other.txt"}]},
        {},
    ]
    uploader, client = _uploader(pages)

    assert uploader.list_dates("osm-changes") == ["2026-04-18", "2026-04-19"]
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="bucket", Prefix="osm-changes/date=")
