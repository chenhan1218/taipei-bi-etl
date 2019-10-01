import datetime
import logging
from argparse import Namespace
from typing import Any, Dict

import pytest
import requests
from google.cloud.storage import Bucket
from pandas import DataFrame

import utils.config
from tasks import rps
from tests.utils import inject_fixtures
from utils.config import get_configs

log = logging.getLogger(__name__)

task = "rps"
inject_fixtures(
    globals(),
    task,
    {
        "prd": utils.config.get_configs(task, ""),
        "dbg": utils.config.get_configs(task, ""),
    },
)
cfg = get_configs(task, "test")


@pytest.mark.envtest
def test_read_api(req: requests, api_src: Dict[str, Any]):
    """Test calling APIs in source configs."""
    r = req.get(api_src["url"], allow_redirects=True)
    assert len(r.text) > 0


@pytest.mark.envtest
def test_write_gcs(gcs_bucket: Bucket, gcs_dest: Dict[str, Any]):
    """Test writing a GCS blob in destination config."""
    blob = gcs_bucket.blob(gcs_dest["prefix"] + "test.txt")
    blob.upload_from_string("This is a test.")
    blob.delete()


"""
# @pytest.mark.unittest
def test_revenue_extract_via_api(mock_io):
    args = Namespace(
        config="test",
        date=datetime.datetime(2019, 9, 8, 0, 0),
        debug=True,
        dest="fs",
        loglevel=None,
        period=30,
        rm=False,
        source="google_search_rps",
        step="e",
        task="rps",
    )
    task = rps.RpsEtlTask(args, cfg.SOURCES, cfg.SCHEMA, cfg.DESTINATIONS)
    df = task.extract_via_api("google_search_rps", cfg.SOURCES["google_search_rps"])
    assert isinstance(df, DataFrame)
    assert df.equals(queryResult)
    assert 1
"""
