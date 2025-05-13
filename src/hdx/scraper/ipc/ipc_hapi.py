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
from hdx.utilities.dateparse import (
    iso_string_from_datetime,
    parse_date,
    parse_date_range,
)
from hdx.utilities.dictandlist import dict_of_lists_add

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
            admin = AdminLevel(
                admin_config=self._configuration["hapi_adm_matching"][
                    f"admin{admin_level}"
                ],
                admin_level=admin_level,
                retriever=self._retriever,
            )
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
        duplicate_check = {}
        analysis_id = 0
        for data_type, rows in self.global_data.items():
            if "wide" not in data_type or "latest" in data_type or "date" in data_type:
                continue
            if "country" in data_type:
                admin_level = 0
                resource_name = "ipc_global_national_wide.csv"
            elif "group" in data_type:
                admin_level = 1
                resource_name = "ipc_global_level1_wide.csv"
            elif "area" in data_type:
                admin_level = 2
                resource_name = "ipc_global_area_wide.csv"
            resource_id = [r["id"] for r in resources if r["name"] == resource_name][0]

            for row in rows:
                countryiso3 = row["Country"]
                hrp = "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
                gho = "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"

                # get admin names and codes
                row_admin_level = admin_level
                warnings = []
                adm_codes = ["", ""]
                adm_names = ["", ""]
                provider_adm_names = ["", ""]

                if admin_level > 0:
                    level1_name = row.get("Level 1", "")
                    area_name = row.get("Area", "")
                    if countryiso3 in adm_matching_config["adm1_only"]:
                        if admin_level == 2:
                            warnings.append("Admin level not present in CODs")
                            provider_adm_names = [level1_name, area_name]
                            match_adm_names = ["", ""]
                        else:
                            provider_adm_names = [level1_name, ""]
                            match_adm_names = [level1_name, ""]
                        self._country_status[countryiso3] = (
                            "Level 1: Admin 1, Area: ignored"
                        )
                    elif countryiso3 in adm_matching_config["adm2_only"]:
                        if admin_level == 1:
                            warnings.append("Admin level not present in CODs")
                            provider_adm_names = [level1_name, ""]
                            match_adm_names = ["", ""]
                        else:
                            self._country_status[countryiso3] = (
                                "Level 1: ignored, Area: Admin 2"
                            )
                            provider_adm_names = [level1_name, area_name]
                            match_adm_names = ["", area_name]
                    elif countryiso3 in adm_matching_config["adm2_only_include_adm1"]:
                        self._country_status[countryiso3] = (
                            "Level 1: Admin 1, Area: Admin 2"
                        )
                        provider_adm_names = [level1_name, area_name]
                        match_adm_names = [level1_name, area_name]
                    elif countryiso3 in adm_matching_config["adm2_in_level1"]:
                        row_admin_level = 2
                        self._country_status[countryiso3] = (
                            "Level 1: Admin 2, Area: ignored"
                        )
                        provider_adm_names = ["", level1_name]
                        match_adm_names = ["", level1_name]
                    elif countryiso3 in adm_matching_config["adm1_in_area"]:
                        if admin_level == 1:
                            warnings.append("Non-matching admin one unit")
                            provider_adm_names = [level1_name, area_name]
                            match_adm_names = ["", ""]
                        else:
                            self._country_status[countryiso3] = (
                                "Level 1: ignored, Area: Admin 1"
                            )
                            provider_adm_names = [area_name, ""]
                            match_adm_names = [area_name, ""]
                            row_admin_level = 1
                    else:
                        provider_adm_names = [level1_name, area_name]
                        match_adm_names = [level1_name, area_name]
                        if not provider_adm_names[0]:
                            if admin_level == 1:
                                self._error_handler.add_message(
                                    "FoodSecurity",
                                    dataset_name,
                                    f"Admin 1: ignoring blank Level 1 name in {countryiso3}",
                                    message_type="warning",
                                )
                                warnings.append("Blank Level 1 name")
                            # "Level 1" and "Area" are used loosely, so admin 1 or admin
                            # 2 data can be in "Area". Usually if "Level 1" is populated,
                            # "Area" is admin 2 and if it isn't, "Area" is admin 1.
                            provider_adm_names = [area_name, ""]
                            match_adm_names = [area_name, ""]
                            row_admin_level = 1
                            if not provider_adm_names[0]:
                                self._error_handler.add_message(
                                    "FoodSecurity",
                                    dataset_name,
                                    f"Admin {admin_level}: ignoring blank Area name in "
                                    f"{countryiso3}",
                                    message_type="warning",
                                )
                                warnings.append("Blank Area name")
                            else:
                                self._country_status[countryiso3] = (
                                    "Level 1: ignored, Area: Admin 1"
                                )
                        else:
                            self._country_status[countryiso3] = (
                                "Level 1: Admin 1, Area: Admin 2"
                            )
                        if not provider_adm_names[1]:
                            row_admin_level = 1

                    full_adm_name = (
                        f"{countryiso3}|{match_adm_names[0]}|{match_adm_names[1]}"
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
                        match_adm_names = ["", ""]
                        warnings.append("Cannot match row")
                    _, additional_warnings = complete_admins(
                        self._admins,
                        countryiso3,
                        match_adm_names,
                        adm_codes,
                        adm_names,
                    )
                    for warning in additional_warnings:
                        warnings.append(warning)

                # loop through projections
                date_of_analysis = parse_date_range(row["Date of analysis"])[0]
                for projection in ["Current", "First projection", "Second projection"]:
                    errors = []
                    population_analyzed = row[
                        f"Population analyzed {projection.lower()}"
                    ]
                    if population_analyzed is None:
                        continue
                    analysis_id += 1
                    time_period_start = row[f"{projection} from"]
                    if time_period_start is None:
                        time_period_end = None
                        errors.append("No time period provided")
                    else:
                        time_period_start = parse_date(time_period_start)
                        time_period_start = iso_string_from_datetime(time_period_start)
                        time_period_end = parse_date(row[f"{projection} to"])
                        time_period_end = iso_string_from_datetime(time_period_end)

                        # check for duplicates
                        primary_key = (
                            countryiso3,
                            adm_codes[0],
                            adm_codes[1],
                            provider_adm_names[0],
                            provider_adm_names[1],
                            projection.lower(),
                            time_period_start,
                        )
                        dict_of_lists_add(
                            duplicate_check,
                            primary_key,
                            (analysis_id, population_analyzed, date_of_analysis),
                        )

                    # loop through phases
                    for phase in ["all", "3+", "1", "2", "3", "4", "5"]:
                        if phase == "all":
                            population_in_phase = population_analyzed
                            population_fraction_in_phase = 1
                        else:
                            population_in_phase = row[
                                f"Phase {phase} number {projection.lower()}"
                            ]
                            population_fraction_in_phase = row[
                                f"Phase {phase} percentage {projection.lower()}"
                            ]
                        if population_in_phase is None:
                            continue
                        hapi_row = {
                            "location_code": countryiso3,
                            "has_hrp": hrp,
                            "in_gho": gho,
                            "provider_admin1_name": provider_adm_names[0],
                            "provider_admin2_name": provider_adm_names[1],
                            "admin1_code": adm_codes[0],
                            "admin1_name": adm_names[0],
                            "admin2_code": adm_codes[1],
                            "admin2_name": adm_names[1],
                            "admin_level": row_admin_level,
                            "ipc_phase": phase,
                            "ipc_type": projection.lower(),
                            "population_in_phase": population_in_phase,
                            "population_fraction_in_phase": population_fraction_in_phase,
                            "reference_period_start": time_period_start,
                            "reference_period_end": time_period_end,
                            "dataset_hdx_id": dataset_id,
                            "resource_hdx_id": resource_id,
                            "warning": "|".join(warnings),
                            "error": "|".join(errors),
                            "date_of_analysis": iso_string_from_datetime(
                                date_of_analysis
                            ),
                            "analysis_id": analysis_id,
                        }
                        hapi_rows.append(hapi_row)

        # find duplicates
        # if there are rows with different dates of analysis,
        # the row with the earlier date is excluded
        # if there are rows with the same date of analysis,
        # the row with the smaller population is excluded
        # values: (analysis_id, population_analyzed, date_of_analysis)
        duplicates = {}
        for _, values in duplicate_check.items():
            if len(values) == 1:
                continue
            dates = [value[2] for value in values]
            latest_date = max(dates)
            for analysis_id, _, analysis_date in values:
                if analysis_date != latest_date:
                    duplicates[analysis_id] = (
                        "Duplicate row with earlier date of analysis excluded"
                    )
            analysis_ids_left = [
                value[0] for value in values if value[0] not in duplicates
            ]
            if len(analysis_ids_left) == 1:
                continue
            populations = [
                value[1] for value in values if value[0] in analysis_ids_left
            ]
            highest_population = max(populations)
            for analysis_id, population, _ in values:
                if population != highest_population and analysis_id not in duplicates:
                    duplicates[analysis_id] = (
                        "Duplicate row with lower population analyzed excluded"
                    )
            population_count = populations.count(highest_population)
            if population_count > 1:
                analysis_ids_same_population = [
                    value[0]
                    for value in values
                    if value[1] == highest_population and value[0] not in duplicates
                ]
                for analysis_id in analysis_ids_same_population[1:]:
                    duplicates[analysis_id] = "Duplicate row excluded"

        for row in hapi_rows:
            analysis_id = row["analysis_id"]
            duplicate_error = duplicates.get(analysis_id)
            if not duplicate_error:
                continue
            errors = row["error"].split("|")
            if errors == [""]:
                errors = []
            errors.append(duplicate_error)
            row["error"] = "|".join(errors)

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
