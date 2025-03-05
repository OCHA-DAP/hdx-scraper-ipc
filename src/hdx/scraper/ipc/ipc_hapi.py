#!/usr/bin/python
"""
IPC:
----

Reads IPC data and creates HAPI datasets.

"""

import logging
from typing import Dict, List

from hdx.data.dataset import Dataset
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.framework.utilities.hapi_admins import complete_admins
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date

logger = logging.getLogger(__name__)


class HAPIOutput:
    def __init__(self, configuration, retriever, folder, error_handler, global_data):
        self._configuration = configuration
        self._retriever = retriever
        self._folder = folder
        self._error_handler = error_handler
        self._admins = []
        self._country_status = {}
        self.global_data = global_data

    def get_pcodes(self) -> None:
        for admin_level in [1, 2]:
            admin = AdminLevel(admin_level=admin_level, retriever=self._retriever)
            dataset = admin.get_libhxl_dataset(retriever=self._retriever)
            admin.setup_from_libhxl_dataset(dataset)
            admin.load_pcode_formats()
            self._admins.append(admin)

    def process_data(self) -> List[Dict]:
        hapi_rows = []
        adm_matching_config = self._configuration["hapi_adm_matching"]
        dataset = Dataset.read_from_hdx("global-acute-food-insecurity-country-data")
        resources = dataset.get_resources()
        dataset_id = dataset["id"]
        dataset_name = dataset["name"]
        for data_type, rows in self.global_data.items():
            if "wide" in data_type or "latest" in data_type:
                continue
            if "country" in data_type:
                admin_level = 0
                resource_name = "ipc_global_national_long.csv"
            elif "group" in data_type:
                admin_level = 1
                resource_name = "ipc_global_level1_long.csv"
            elif "area" in data_type:
                admin_level = 2
                resource_name = "ipc_global_area_long.csv"
            resource_id = [r["id"] for r in resources if r["name"] == resource_name][0]

            for row in rows:
                countryiso3 = row["Country"]
                hrp = "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
                gho = "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"

                time_period_start = parse_date(row["From"])
                time_period_end = parse_date(row["To"])

                warnings = []
                errors = []
                adm_codes = ["", ""]
                adm_names = ["", ""]

                if admin_level > 0:
                    if countryiso3 in adm_matching_config["adm1_only"]:
                        if admin_level == 2:
                            self._country_status[countryiso3] = (
                                "Level 1: Admin 1, Area: ignored"
                            )
                        provider_adm_names = [row.get("Level 1", ""), ""]
                    elif countryiso3 in adm_matching_config["adm2_only"]:
                        if admin_level == 1:
                            warnings.append(f"{countryiso3} only has admin two data")
                            provider_adm_names = ["", ""]
                        else:
                            self._country_status[countryiso3] = (
                                "Level 1: ignored, Area: Admin 2"
                            )
                            provider_adm_names = ["", row.get("Area", "")]
                    elif countryiso3 in adm_matching_config["adm2_in_level1"]:
                        self._country_status[countryiso3] = "Level 1: Admin 2, Area: ignored"
                        provider_adm_names = ["", row.get("Level 1", "")]
                    elif countryiso3 in adm_matching_config["adm1_in_area"]:
                        if admin_level == 1:
                            warnings.append(
                                f"{countryiso3} has non-matching admin one admin units"
                            )
                            provider_adm_names = ["", ""]
                        else:
                            self._country_status[countryiso3] = (
                                "Level 1: ignored, Area: Admin 1"
                            )
                            provider_adm_names = [row.get("Area", ""), ""]
                    else:
                        provider_adm_names = [row.get("Level 1", ""), row.get("Area", "")]
                        if not provider_adm_names[0]:
                            if admin_level == 1:
                                self._error_handler.add_message(
                                    "FoodSecurity",
                                    dataset_name,
                                    f"Admin 1: ignoring blank Level 1 name in {countryiso3}",
                                    message_type="warning",
                                )
                                provider_adm_names = ["", ""]
                                warnings.append(f"{countryiso3} has blank Level 1 name")
                            else:
                                # "Level 1" and "Area" are used loosely, so admin 1 or admin
                                # 2 data can be in "Area". Usually if "Level 1" is populated,
                                # "Area" is admin 2 and if it isn't, "Area" is admin 1.
                                provider_adm_names = [row.get("Area", ""), ""]
                                if not provider_adm_names[0]:
                                    self._error_handler.add_message(
                                        "FoodSecurity",
                                        dataset_name,
                                        f"Admin 1: ignoring blank Area name in {countryiso3}",
                                        message_type="warning",
                                    )
                                    warnings.append(f"{countryiso3} has blank Area name")
                        else:
                            self._country_status[countryiso3] = (
                                "Level 1: Admin 1, Area: Admin 2"
                            )

                    # TODO: deal with adm1_errors, adm2_errors
                    full_adm_name = (
                        f"{countryiso3}|{provider_adm_names[0]}|{provider_adm_names[1]}"
                    )
                    if any(
                        x in full_adm_name.lower()
                        for x in adm_matching_config["adm_ignore_patterns"]
                    ):
                        self._error_handler.add_message(
                            "FoodSecurity",
                            dataset_name,
                            f"Ignoring {full_adm_name}",
                            message_type="warning",
                        )
                        provider_adm_names = ["", ""]
                        warnings.append("Not matching row")
                    try:
                        _, additional_warnings = complete_admins(
                            self._admins,
                            countryiso3,
                            provider_adm_names,
                            adm_codes,
                            adm_names,
                        )
                    except IndexError:
                        adm_codes = ["", ""]
                        additional_warnings = [f"PCode unknown {adm_codes[1]}->''"]
                    for warning in additional_warnings:
                        warnings.append(warning)

                hapi_row = {
                    "location_code": countryiso3,
                    "has_hrp": hrp,
                    "in_gho": gho,
                    "provider_admin1_name": row.get("Level 1"),
                    "provider_admin2_name": row.get("Area"),
                    "admin1_code": adm_codes[0],
                    "admin1_name": adm_names[0],
                    "admin2_code": adm_codes[1],
                    "admin2_name": adm_names[1],
                    "admin_level": admin_level,
                    "ipc_phase": row["Phase"],
                    "ipc_type": row["Validity period"],
                    "population_in_phase": row["Number"],
                    "population_fraction_in_phase": row["Percentage"],
                    "reference_period_start": iso_string_from_datetime(time_period_start),
                    "reference_period_end": iso_string_from_datetime(time_period_end),
                    "dataset_hdx_id": dataset_id,
                    "resource_hdx_id": resource_id,
                    "warning": "|".join(warnings),
                    "error": "|".join(errors),
                }
                hapi_rows.append(hapi_row)

        logger.info(f"{dataset_name} - Country Status")
        for countryiso3 in sorted(self._country_status):
            status = self._country_status[countryiso3]
            logger.info(f"{countryiso3}: {status}")
        return hapi_rows

    def generate_dataset(self):
        self.get_pcodes()
        dataset = Dataset(
            {
                "name": "hdx-hapi-food-security",
                "title": "HDX HAPI - Food Security, Nutrition & Poverty: Food Security",
            }
        )
        dataset.add_tags(["food security", "hxl"])
        dataset.add_other_location("world")
        start_date = self.global_data["start_date"]
        end_date = self.global_data["end_date"]
        dataset.set_time_period(start_date, end_date)

        hxl_tags = self._configuration["hapi_hxltags"]
        headers = list(hxl_tags.keys())
        data = self.process_data()
        dataset.generate_resource_from_iterable(
            headers,
            data,
            hxl_tags,
            self._folder,
            "hdx_hapi_food_security_global.csv",
            self._configuration["hapi_resource"],
            encoding="utf-8-sig",
        )

        return dataset
