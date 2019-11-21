from copy import deepcopy
from configs import bigquery
from utils.config import merge_config

BQ_PROJECT = {
    "project": "taipei-bi",
    "location": "US",
    "dataset": "chenhan_dev",
}


def set_debug_config():
    c = {
        key: value
        for key, value in bigquery.__dict__.items()
        if (
           not (key.startswith("__") or key.startswith("_") or key == "BQ_PROJECT")
        ) and isinstance(value, dict)
    }
    for k, v in c.items():
        v_dbg = deepcopy(v)
        merge_config(v_dbg["params"], BQ_PROJECT)
        globals()[k] = v_dbg


set_debug_config()

globals()["MANGO_EVENTS"]["params"]["src"] = "rocket-dev01.mango_dev.mango_events"
globals()["MANGO_CORE"]["params"]["src"] = "rocket-dev01.mango_dev.mango_core"
