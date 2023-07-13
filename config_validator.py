# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Processes and validates SAP Reporting config.json.
"""

import logging
from typing import Union


def validate(cfg: dict) -> Union[dict, None]:
    """Validates and processes configuration.

    Args:
        cfg (dict): Config dictionary.

    Returns:
        dict: Processed config dictionary.
    """
    if not cfg.get("deploySAP", False):
        return cfg
    sap = cfg.get("SAP", None)

    if not sap:
        logging.error("Missing 'SAP' values in the config file.")
        return None

    deploy_cdc = sap.get("deployCDC")
    if deploy_cdc is None:
        logging.error("Missing 'SAP/deployCDC' values in the config file.")
        return None

    datasets = sap.get("datasets")
    if not datasets:
        logging.error("Missing 'SAP/datasets' values in the config file.")
        return None

    cfg["SAP"]["SQLFlavor"] = sap.get("SQLFlavor", "ecc").lower()
    flavor = cfg["SAP"]["SQLFlavor"]

    cfg_sap_ds_cdc = datasets.get("cdc", None)
    cfg_sap_ds_cdc_ecc = datasets.get("cdcECC", None)
    cfg_sap_ds_cdc_s4 = datasets.get("cdcS4", None)

    if flavor == "ecc":
        if cfg_sap_ds_cdc and not cfg_sap_ds_cdc_ecc:
            cfg_sap_ds_cdc_ecc = cfg_sap_ds_cdc
        else:
            cfg_sap_ds_cdc = cfg_sap_ds_cdc_ecc
    elif flavor == "s4":
        if cfg_sap_ds_cdc and not cfg_sap_ds_cdc_s4:
            cfg_sap_ds_cdc_s4 = cfg_sap_ds_cdc
        else:
            cfg_sap_ds_cdc = cfg_sap_ds_cdc_s4
    if not cfg_sap_ds_cdc:
        logging.error(("Cannot resolve SAP/datasets/cdc|cdcECC|cdcS4 values "
                       "in the config file."))
        return None
    cfg["SAP"]["datasets"]["cdc"] = cfg_sap_ds_cdc
    cfg["SAP"]["datasets"]["cdcECC"] = cfg_sap_ds_cdc_ecc
    cfg["SAP"]["datasets"]["cdcS4"] = cfg_sap_ds_cdc_s4

    cfg_sap_ds_raw = datasets.get("raw", None)
    cfg_sap_ds_raw_ecc = datasets.get("rawECC", None)
    cfg_sap_ds_raw_s4 = datasets.get("rawS4", None)

    if flavor == "union":
        if not cfg_sap_ds_raw_ecc or not cfg_sap_ds_raw_s4:
            logging.error("ERROR: 🛑🔪 SAP/SQLFlavor=union requires "
                          "all parameters for both ECC and S4 🔪🛑")
    elif flavor == "ecc":
        if cfg_sap_ds_raw and not cfg_sap_ds_raw_ecc:
            cfg_sap_ds_raw_ecc = cfg_sap_ds_raw
        else:
            cfg_sap_ds_raw = cfg_sap_ds_raw_ecc
    elif flavor == "s4":
        if cfg_sap_ds_raw and not cfg_sap_ds_raw_s4:
            cfg_sap_ds_raw_s4 = cfg_sap_ds_raw
        else:
            cfg_sap_ds_raw = cfg_sap_ds_raw_s4
    if not cfg_sap_ds_raw:
        logging.error(("Cannot resolve SAP/datasets/raw|rawECC|rawS4 values "
                       "in the config file."))
        return None
    cfg["SAP"]["datasets"]["raw"] = cfg_sap_ds_raw
    cfg["SAP"]["datasets"]["rawECC"] = cfg_sap_ds_raw_ecc
    cfg["SAP"]["datasets"]["rawS4"] = cfg_sap_ds_raw_s4

    cfg["SAP"]["datasets"]["reporting"] = datasets.get("reporting", "REPORTING")
    cfg["SAP"]["datasets"]["ml"] = datasets.get("ml", "ML_MODELS")

    cfg_sap_mandt = sap.get("mandt", None)
    cfg_sap_mandt_ecc = sap.get("mandtECC", None)
    cfg_sap_mandt_s4 = sap.get("mandtS4", None)

    if flavor == "union":
        if not cfg_sap_mandt_ecc or not cfg_sap_mandt_s4:
            logging.error(("ERROR: 🛑🔪 SAP/SQLFlavor=union requires "
                           "all parameters for both ECC and S4 🔪🛑"))
            return None
        elif cfg_sap_mandt_ecc == cfg_sap_mandt_s4:
            logging.error(("ERROR: 🛑🔪 Same ECC and S4 MANDT "
                           "is not allowed for UNION workloads"))
            return None
    elif flavor == "ecc":
        if cfg_sap_mandt and not cfg_sap_mandt_ecc:
            cfg_sap_mandt_ecc = cfg_sap_mandt
        else:
            cfg_sap_mandt = cfg_sap_mandt_ecc
    elif flavor == "s4":
        if cfg_sap_mandt and not cfg_sap_mandt_s4:
            cfg_sap_mandt_s4 = cfg_sap_mandt
        else:
            cfg_sap_mandt = cfg_sap_mandt_s4
    if not cfg_sap_mandt:
        logging.warning("Using default SAP Mandt/client = 100.")
        cfg_sap_mandt = "100"
    cfg["SAP"]["mandt"] = cfg_sap_mandt
    cfg["SAP"]["mandtECC"] = cfg_sap_mandt_ecc or cfg_sap_mandt
    cfg["SAP"]["mandtS4"] = cfg_sap_mandt_s4 or cfg_sap_mandt

    return cfg
