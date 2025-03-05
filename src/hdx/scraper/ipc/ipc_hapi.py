#!/usr/bin/python
"""
IPC:
----

Reads IPC data and creates HAPI datasets.

"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from hdx.data.dataset import Dataset
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.scraper.framework.utilities.hapi_admins import complete_admins
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.typehint import ListTuple

logger = logging.getLogger(__name__)


@dataclass
class AdminInfo:
    countryiso3: str
    name: str
    fullname: str
    pcode: Optional[str]
    exact: bool


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

    def get_adminoneinfo(
        self,
        adm_ignore_patterns: ListTuple,
        dataset_name: str,
        countryiso3: str,
        adminone_name: str,
    ) -> Optional[AdminInfo]:
        full_adm1name = f"{countryiso3}|{adminone_name}"
        if any(x in adminone_name.lower() for x in adm_ignore_patterns):
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 1: ignoring {full_adm1name}",
                message_type="warning",
            )
            return None
        pcode, exact = self._adminone.get_pcode(countryiso3, adminone_name)
        return AdminInfo(countryiso3, adminone_name, full_adm1name, pcode, exact)

    def get_admintwoinfo(
        self,
        adm_ignore_patterns: ListTuple,
        dataset_name: str,
        adminoneinfo: AdminInfo,
        admintwo_name: str,
    ) -> Optional[AdminInfo]:
        full_adm2name = f"{adminoneinfo.countryiso3}|{adminoneinfo.name}|{admintwo_name}"
        if any(x in admintwo_name.lower() for x in adm_ignore_patterns):
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 2: ignoring {full_adm2name}",
                message_type="warning",
            )
            return None
        pcode, exact = self._admintwo.get_pcode(
            adminoneinfo.countryiso3, admintwo_name, parent=adminoneinfo.pcode
        )
        return AdminInfo(
            adminoneinfo.countryiso3,
            admintwo_name,
            full_adm2name,
            pcode,
            exact,
        )

    def get_adminone_admin2_ref(
        self,
        food_sec_config: Dict,
        dataset_name: str,
        adminoneinfo: AdminInfo,
    ) -> Optional[int]:
        if not adminoneinfo.pcode:
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 1: could not match {adminoneinfo.fullname}!",
                message_type="warning",
            )
            return None
        if not adminoneinfo.exact:
            name = self._adminone.pcode_to_name[adminoneinfo.pcode]
            if adminoneinfo.name in food_sec_config["adm1_errors"]:
                self._error_handler.add_message(
                    "FoodSecurity",
                    dataset_name,
                    f"Admin 1: ignoring erroneous {adminoneinfo.fullname} match to {name} {(adminoneinfo.pcode)}!",
                )
                return None
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 1: matching {adminoneinfo.fullname} to {name} {(adminoneinfo.pcode)}",
                message_type="warning",
            )
        return self._admins.get_admin2_ref(
            "adminone",
            adminoneinfo.pcode,
            dataset_name,
            "FoodSecurity",
            self._error_handler,
        )

    def get_admintwo_admin2_ref(
        self,
        food_sec_config: Dict,
        dataset_name: str,
        row: Dict,
        adminoneinfo: AdminInfo,
    ) -> Optional[int]:
        admintwo_name = row["Area"]
        if not admintwo_name:
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 1: ignoring blank Area name in {adminoneinfo.countryiso3}|{adminoneinfo.name}",
                message_type="warning",
            )
            return None
        admintwoinfo = self.get_admintwoinfo(
            food_sec_config["adm_ignore_patterns"],
            dataset_name,
            adminoneinfo,
            admintwo_name,
        )
        if not admintwoinfo:
            return None
        if not admintwoinfo.pcode:
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 2: could not match {admintwoinfo.fullname}!",
                message_type="warning",
            )
            return None
        if not admintwoinfo.exact:
            name = self._admintwo.pcode_to_name[admintwoinfo.pcode]
            if admintwo_name in food_sec_config["adm2_errors"]:
                self._error_handler.add_message(
                    "FoodSecurity",
                    dataset_name,
                    f"Admin 2: ignoring erroneous {admintwoinfo.fullname} match to {name} {(admintwoinfo.pcode)}!",
                )
                return None
            self._error_handler.add_message(
                "FoodSecurity",
                dataset_name,
                f"Admin 2: matching {admintwoinfo.fullname} to {name} {(admintwoinfo.pcode)}",
                message_type="warning",
            )
        return self._admins.get_admin2_ref(
            "admintwo",
            admintwoinfo.pcode,
            dataset_name,
            "FoodSecurity",
            self._error_handler,
        )

    def process_subnational(
        self,
        food_sec_config: Dict,
        dataset_name: str,
        countryiso3: str,
        admin_level: str,
        row: Dict,
    ) -> Optional[int]:
        # Some countries only have data in the ipc_global_level1 file
        if admin_level == "admintwo" and countryiso3 in food_sec_config["adm1_only"]:
            self._country_status[countryiso3] = "Level 1: Admin 1, Area: ignored"
            adminoneinfo = self.get_adminoneinfo(
                food_sec_config["adm_ignore_patterns"],
                dataset_name,
                countryiso3,
                row["Level 1"],
            )
            return self.get_adminone_admin2_ref(
                food_sec_config,
                dataset_name,
                adminoneinfo,
            )
        # The YAML configuration "adm2_only" specifies locations where
        # "Level 1" is not populated and "Area" is admin 2. (These are
        # exceptions since "Level 1" would normally be populated if "Area" is
        # admin 2.)
        if countryiso3 in food_sec_config["adm2_only"]:
            # Some countries only have data in the ipc_global_area file
            if admin_level == "adminone":
                return None
            adminoneinfo = AdminInfo(countryiso3, "NOT GIVEN", "", None, False)
            self._country_status[countryiso3] = "Level 1: ignored, Area: Admin 2"
            return self.get_admintwo_admin2_ref(
                food_sec_config,
                dataset_name,
                row,
                adminoneinfo,
            )

        if countryiso3 in food_sec_config["adm2_in_level1"]:
            row["Area"] = row["Level 1"]
            row["Level 1"] = None
            adminoneinfo = AdminInfo(countryiso3, "NOT GIVEN", "", None, False)
            self._country_status[countryiso3] = "Level 1: Admin 2, Area: ignored"
            return self.get_admintwo_admin2_ref(
                food_sec_config,
                dataset_name,
                row,
                adminoneinfo,
            )

        if countryiso3 in food_sec_config["adm1_in_area"]:
            if admin_level == "adminone":
                return None
            self._country_status[countryiso3] = "Level 1: ignored, Area: Admin 1"
            adminoneinfo = self.get_adminoneinfo(
                food_sec_config["adm_ignore_patterns"],
                dataset_name,
                countryiso3,
                row["Area"],
            )
            return self.get_adminone_admin2_ref(
                food_sec_config,
                dataset_name,
                adminoneinfo,
            )

        adminone_name = row["Level 1"]

        if not adminone_name:
            if admin_level == "adminone":
                if not adminone_name:
                    self._error_handler.add_message(
                        "FoodSecurity",
                        dataset_name,
                        f"Admin 1: ignoring blank Level 1 name in {countryiso3}",
                        message_type="warning",
                    )
                    return None
            else:
                # "Level 1" and "Area" are used loosely, so admin 1 or admin 2 data can
                # be in "Area". Usually if "Level 1" is populated, "Area" is admin 2
                # and if it isn't, "Area" is admin 1.
                adminone_name = row["Area"]
                if not adminone_name:
                    self._error_handler.add_message(
                        "FoodSecurity",
                        dataset_name,
                        f"Admin 1: ignoring blank Area name in {countryiso3}",
                        message_type="warning",
                    )
                    return None
                adminoneinfo = self.get_adminoneinfo(
                    food_sec_config["adm_ignore_patterns"],
                    dataset_name,
                    countryiso3,
                    adminone_name,
                )
                if not adminoneinfo:
                    return None
                self._country_status[countryiso3] = "Level 1: ignored, Area: Admin 1"
                return self.get_adminone_admin2_ref(
                    food_sec_config,
                    dataset_name,
                    adminoneinfo,
                )

        adminoneinfo = self.get_adminoneinfo(
            food_sec_config["adm_ignore_patterns"],
            dataset_name,
            countryiso3,
            adminone_name,
        )
        if not adminoneinfo:
            return None
        if countryiso3 in food_sec_config["adm1_only"]:
            self._country_status[countryiso3] = "Level 1: Admin 1, Area: ignored"
        else:
            self._country_status[countryiso3] = "Level 1: Admin 1, Area: Admin 2"
        if admin_level == "adminone":
            return self.get_adminone_admin2_ref(food_sec_config, dataset_name, adminoneinfo)
        return self.get_admintwo_admin2_ref(
            food_sec_config,
            dataset_name,
            row,
            adminoneinfo,
        )

    def process_data(self) -> List[Dict]:
        hapi_rows = []
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

            # Date of analysis,Country,Total country population,Level 1,Area,Validity period,From,To,Phase,Number,Percentage
            for row in rows:
                countryiso3 = row["Country"]
                hrp = "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
                gho = "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"

                time_period_start = parse_date(row["From"])
                time_period_end = parse_date(row["To"])

                warnings = []
                errors = []
                provider_adm_names = [
                    row.get("Level 1"),
                    row.get("Area"),
                ]
                adm_codes = ["", ""]
                adm_names = ["", ""]

                if admin_level > 0:
                    admin2_ref = self.process_subnational(
                        self._configuration["hapi_adm_matching"],
                        dataset_name,
                        countryiso3,
                        admin_level,
                        row,
                    )

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
