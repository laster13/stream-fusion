import json

from stream_fusion.utils.string_encoding import decrypt_config
from stream_fusion.logging_config import logger


def parse_config(config_token):
    config = json.loads(decrypt_config(config_token))

    if "languages" not in config:
        config["languages"] = [config["language"]]

    if "jackett" not in config:
        config["jackett"] = False

    if isinstance(config.get("RDToken"), str):
        try:
            config["RDToken"] = json.loads(config["RDToken"])
        except json.JSONDecodeError:
            pass

    if "anonymizeMagnets" not in config:
        config["anonymizeMagnets"] = False

    if "addonHost" not in config:
        logger.warning("addonHost not found in config, using default")
        config["addonHost"] = "http://127.0.0.1:8000"

    if "c411" not in config:
        config["c411"] = True

    if "torr9" not in config:
        config["torr9"] = True

    if "lacale" not in config:
        config["lacale"] = True

    if "yggflix" not in config:
        config["yggflix"] = True

    if "generationfree" not in config:
        config["generationfree"] = True

    if "abn" not in config:
        config["abn"] = True

    if "g3mini" not in config:
        config["g3mini"] = True

    if "theoldschool" not in config:
        config["theoldschool"] = True

    return config
