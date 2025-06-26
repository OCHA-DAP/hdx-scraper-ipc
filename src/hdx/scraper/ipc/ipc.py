#!/usr/bin/python
"""
IPC:
----

Reads IPC data and creates datasets.

"""

import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List

from dateutil.relativedelta import relativedelta
from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
)
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class IPC:
    def __init__(
        self,
        configuration: Configuration,
        retriever: Retrieve,
        state: Dict,
        ch_countries: List,
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._state = state
        self._default_start_date = state["DEFAULT"]
        self._base_url = configuration["base_url"]
        self._projection_names = [
            "Current",
            "First projection",
            "Second projection",
        ]
        self._projection_suffixes = ["", "_projected", "_second_projected"]
        self._projections = ["current", "projected", "second_projected"]
        self._phasemapping = {
            "estimated": "all",
            "p3plus": "3+",
            "phase1": "1",
            "phase2": "2",
            "phase3": "3",
            "phase4": "4",
            "phase5": "5",
        }
        self._colnamemapping = {"estimated": "analyzed"}
        self._output = {
            "country_rows_latest": [],
            "country_rows_wide_latest": [],
            "group_rows_latest": [],
            "group_rows_wide_latest": [],
            "area_rows_latest": [],
            "area_rows_wide_latest": [],
            "country_rows": [],
            "country_rows_wide": [],
            "group_rows": [],
            "group_rows_wide": [],
            "area_rows": [],
            "area_rows_wide": [],
            "start_date": state.get("START_DATE", default_enddate),
            "end_date": state.get("END_DATE", default_date),
        }
        name, title = self.get_dataset_title_name("Global")
        temp_dataset = Dataset({"name": name, "title": title})
        self._global_dataset_url = temp_dataset.get_hdx_url()
        self._ch_countries = ch_countries

    def get_dataset_title_name(self, countryname):
        title = f"{countryname}: Acute Food Insecurity Country Data"
        name = slugify(title).lower()
        return name, title

    def get_countries(self):
        countryisos = set()
        json = self._retriever.download_json(f"{self._base_url}/analyses?type=A")
        for analysis in json:
            countryiso2 = analysis["country"]
            countryiso3 = Country.get_iso3_from_iso2(countryiso2)
            if countryiso3 is None:
                logger.error(
                    f"Could not find country ISO 3 code matching ISO 2 code {countryiso2}!"
                )
            else:
                countryisos.add(countryiso3)
        return [{"iso3": x} for x in sorted(countryisos)]

    @staticmethod
    def parse_date(datestring):
        date = datetime.strptime(datestring, "%b %Y")
        return date.replace(tzinfo=timezone.utc)

    @classmethod
    def parse_date_range(cls, date_range, time_period):
        start, end = date_range.split(" - ")
        startdate = cls.parse_date(start)
        if startdate < time_period["start_date"]:
            time_period["start_date"] = startdate
        enddate = cls.parse_date(end)
        enddate = enddate + relativedelta(months=1, days=-1)
        if enddate > time_period["end_date"]:
            time_period["end_date"] = enddate
        startdatestr = startdate.date().isoformat()
        enddatestr = enddate.date().isoformat()
        return startdatestr, enddatestr

    def add_country_subnational_rows(
        self,
        base_row,
        time_period,
        location,
        rows,
        rows_wide,
        analysis=None,
    ):
        if analysis is None:
            analysis = location
        country_subnational_row = deepcopy(base_row)
        row_wide = deepcopy(country_subnational_row)
        for i, projection in enumerate(self._projections):
            projection_row = deepcopy(country_subnational_row)
            period_date = analysis.get(f"{projection}_period_dates")
            if period_date:
                period_start, period_end = self.parse_date_range(
                    period_date, time_period
                )
            else:
                period_start = period_end = None
            projection_name = self._projection_names[i]
            projection_name_l = projection_name.lower()
            projection_row["Validity period"] = projection_name_l
            projection_row["From"] = period_start
            projection_row["To"] = period_end
            row_wide[f"{projection_name} from"] = period_start
            row_wide[f"{projection_name} to"] = period_end
            projection_suffix = self._projection_suffixes[i]
            location[f"estimated_percentage{projection_suffix}"] = 1.0
            for prefix, phase in self._phasemapping.items():
                row = deepcopy(projection_row)
                if phase == "3+":
                    key = f"p3plus{projection_suffix}"
                else:
                    key = f"{prefix}_population{projection_suffix}"
                affected = location.get(key)
                row["Phase"] = phase
                row["Number"] = affected
                projection_name_l = projection_name.lower()
                if phase == "all":
                    colname = f"Population analyzed {projection_name_l}"
                else:
                    colname = f"Phase {phase} number {projection_name_l}"
                row_wide[colname] = affected
                percentage = location.get(f"{prefix}_percentage{projection_suffix}")
                row["Percentage"] = percentage
                if prefix != "estimated":
                    row_wide[f"Phase {phase} percentage {projection_name_l}"] = (
                        percentage
                    )
                if affected is not None and period_date:
                    rows.append(row)

        rows_wide.append(row_wide)

    @staticmethod
    def get_base_row(analysis, countryiso3):
        return {
            "Date of analysis": analysis["analysis_date"],
            "Country": countryiso3,
            "Total country population": analysis.get("population"),
        }

    def add_country_rows(self, analysis, countryiso3, time_period, rows, rows_wide):
        base_row = self.get_base_row(analysis, countryiso3)
        self.add_country_subnational_rows(
            base_row,
            time_period,
            analysis,
            rows=rows,
            rows_wide=rows_wide,
        )

    def add_subnational_rows(
        self,
        analysis,
        countryiso3,
        time_period,
        group_rows,
        group_rows_wide,
        area_rows,
        area_rows_wide,
    ):
        def process_areas(adm_row, adm):
            if adm["areas"] is None:
                logger.error(
                    f'{countryiso3}: {analysis["title"]} has blank "areas" field!'
                )
                return
            for area in adm["areas"]:
                area_row = deepcopy(adm_row)
                if "Level 1" not in area_row:
                    area_row["Level 1"] = None
                area_row["Area"] = area["name"]
                self.add_country_subnational_rows(
                    area_row,
                    time_period,
                    area,
                    rows=area_rows,
                    rows_wide=area_rows_wide,
                    analysis=analysis,
                )

        base_row = self.get_base_row(analysis, countryiso3)
        groups = analysis.get("groups")
        if groups:
            for group in analysis["groups"]:
                group_row = deepcopy(base_row)
                group_row["Level 1"] = group["name"]
                self.add_country_subnational_rows(
                    group_row,
                    time_period,
                    group,
                    rows=group_rows,
                    rows_wide=group_rows_wide,
                    analysis=analysis,
                )
                if "areas" in group:
                    process_areas(group_row, group)
        else:
            process_areas(base_row, analysis)

    def get_country_data(self, countryiso3):
        countryiso2 = Country.get_iso2_from_iso3(countryiso3)
        url = f"{self._base_url}/population?country={countryiso2}"
        country_data = self._retriever.download_json(url)
        if not country_data:
            return None
        most_recent_analysis = country_data[0]

        analysis_date = self.parse_date(most_recent_analysis["analysis_date"])
        if analysis_date <= self._state.get(countryiso3, self._default_start_date):
            update = False
        else:
            update = True
        self._state[countryiso3] = analysis_date
        time_period = {"start_date": default_enddate, "end_date": default_date}

        output = {"countryiso3": countryiso3}

        most_recent_current_analysis = None
        for analysis in country_data:
            if analysis["current_period_dates"]:
                most_recent_current_analysis = analysis
                break
        if most_recent_current_analysis:
            analysis_id = most_recent_current_analysis["id"]
            year = self.parse_date(most_recent_current_analysis["analysis_date"]).year
            url = f"{self._base_url}/areas/{analysis_id}/P?country={countryiso2}&year={year}&type=A&format=geojson"
            filename = f"ipc_{countryiso3.lower()}.geojson"
            path = self._retriever.download_file(url, filename=filename)
            output["geojson"] = path
            country_rows = output["country_rows_latest"] = []
            country_rows_wide = output["country_rows_wide_latest"] = []
            group_rows = output["group_rows_latest"] = []
            group_rows_wide = output["group_rows_wide_latest"] = []
            area_rows = output["area_rows_latest"] = []
            area_rows_wide = output["area_rows_wide_latest"] = []
            self.add_country_rows(
                most_recent_current_analysis,
                countryiso3,
                time_period,
                country_rows,
                country_rows_wide,
            )
            self.add_subnational_rows(
                most_recent_current_analysis,
                countryiso3,
                time_period,
                group_rows,
                group_rows_wide,
                area_rows,
                area_rows_wide,
            )
            self._output["country_rows_latest"].extend(country_rows)
            self._output["country_rows_wide_latest"].extend(country_rows_wide)
            self._output["group_rows_latest"].extend(group_rows)
            self._output["group_rows_wide_latest"].extend(group_rows_wide)
            self._output["area_rows_latest"].extend(area_rows)
            self._output["area_rows_wide_latest"].extend(area_rows_wide)
        else:
            output["geojson"] = None

        country_rows = output["country_rows"] = []
        country_rows_wide = output["country_rows_wide"] = []
        group_rows = output["group_rows"] = []
        group_rows_wide = output["group_rows_wide"] = []
        area_rows = output["area_rows"] = []
        area_rows_wide = output["area_rows_wide"] = []
        for analysis in country_data:
            self.add_country_rows(
                analysis,
                countryiso3,
                time_period,
                country_rows,
                country_rows_wide,
            )
            self.add_subnational_rows(
                analysis,
                countryiso3,
                time_period,
                group_rows,
                group_rows_wide,
                area_rows,
                area_rows_wide,
            )
        self._output["country_rows"].extend(country_rows)
        self._output["country_rows_wide"].extend(country_rows_wide)
        self._output["group_rows"].extend(group_rows)
        self._output["group_rows_wide"].extend(group_rows_wide)
        self._output["area_rows"].extend(area_rows)
        self._output["area_rows_wide"].extend(area_rows_wide)

        start_date = time_period["start_date"]
        end_date = time_period["end_date"]
        output["start_date"] = start_date
        output["end_date"] = end_date
        if start_date < self._output["start_date"]:
            self._output["start_date"] = start_date
            self._state["START_DATE"] = start_date
        if end_date > self._output["end_date"]:
            self._output["end_date"] = end_date
            self._state["END_DATE"] = end_date
        if not update:
            return None
        return output

    def get_all_data(self):
        return self._output

    def generate_dataset_and_showcase(self, folder, output):
        if not output:
            return None, None
        countryiso3 = output.get("countryiso3")
        if countryiso3:
            countryname = Country.get_country_name_from_iso3(countryiso3)
            notes = f"There is also a [global dataset]({self._global_dataset_url})."
        else:
            if not output["country_rows_latest"]:
                return None, None
            countryname = "Global"
            notes = (
                f"There are also [country datasets]({self._configuration.get_hdx_site_url()}/"
                f"organization/da501ffc-aadb-43f5-9d28-8fa572fd9ce0)"
            )
        name, title = self.get_dataset_title_name(countryname)
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset(
            {
                "name": name,
                "title": title,
                "notes": notes,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("da501ffc-aadb-43f5-9d28-8fa572fd9ce0")
        dataset.set_expected_update_frequency("As needed")
        dataset.set_subnational(True)
        if countryiso3:
            dataset.add_country_location(countryiso3)
            countryiso3lower = countryiso3.lower()
        else:
            dataset.add_other_location("world")
            countryiso3lower = "global"
        tags = (
            "hxl",
            "food security",
            "integrated food security phase classification-ipc",
        )
        dataset.add_tags(tags)
        dataset.set_time_period(output["start_date"], output["end_date"])

        if countryiso3:
            filename = f"ipc_{countryiso3lower}.geojson"
            resourcedata = {
                "name": filename,
                "description": "IPC GeoJSON for latest analysis",
            }
            resource = Resource(resourcedata)
            resource.set_file_to_upload(output["geojson"])
            resource.set_format("geojson")
            dataset.add_update_resource(resource)

        filename = f"ipc_{countryiso3lower}_national_long_latest.csv"
        resourcedata = {
            "name": filename,
            "description": "Latest IPC national data in long form with HXL tags",
        }
        country_rows = output["country_rows_latest"]
        if not country_rows:
            logger.warning(f"{filename} has no data!")
            return None, None
        success, results = dataset.generate_resource_from_iterable(
            list(country_rows[0].keys()),
            country_rows,
            self._configuration["long_hxltags"],
            folder,
            filename,
            resourcedata,
        )
        if success is False:
            logger.warning(f"{filename} has no data!")
            return None, None

        country_rows_wide = output["country_rows_wide_latest"]
        # Won't do wide latest for country as just one row, but do it for global
        if len(country_rows_wide) > 1:
            filename = f"ipc_{countryiso3lower}_national_wide_latest.csv"
            resourcedata = {
                "name": filename,
                "description": "Latest IPC national data in wide form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
                list(country_rows_wide[0].keys()),
                country_rows_wide,
                self._configuration["wide_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        if countryiso3lower == "global":
            showcase_description = "IPC-CH Dashboard"
            showcase_url = "https://www.ipcinfo.org/ipcinfo-website/ipc-dashboard/en/"
        elif countryiso3 in self._ch_countries:
            showcase_description = (
                "CH regional page on IPC website with map and reports"
            )
            showcase_url = self._configuration["ch_showcase_url"]
        else:
            showcase_description = f"Access all of IPC’s analyses for {countryname}"
            showcase_url = self._configuration["showcase_url"]
            showcase_url = f"{showcase_url}{countryiso3}"
        showcase = Showcase(
            {
                "name": f"{name}-showcase",
                "title": f"{title} showcase",
                "notes": showcase_description,
                "url": showcase_url,
                "image_url": "https://www.ipcinfo.org/fileadmin/user_upload/ipcinfo/img/dashboard_thumbnail.jpg",
            }
        )
        showcase.add_tags(tags)
        group_rows = output["group_rows_latest"]
        if group_rows:
            filename = f"ipc_{countryiso3lower}_level1_long_latest.csv"
            resourcedata = {
                "name": filename,
                "description": "Latest IPC level 1 data in long form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
                list(group_rows[0].keys()),
                group_rows,
                self._configuration["long_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        group_rows_wide = output["group_rows_wide_latest"]
        if group_rows_wide:
            filename = f"ipc_{countryiso3lower}_level1_wide_latest.csv"
            resourcedata = {
                "name": filename,
                "description": "Latest IPC level 1 data in wide form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
                list(group_rows_wide[0].keys()),
                group_rows_wide,
                self._configuration["wide_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        area_rows = output["area_rows_latest"]
        if area_rows:
            filename = f"ipc_{countryiso3lower}_area_long_latest.csv"
            resourcedata = {
                "name": filename,
                "description": "Latest IPC area data in long form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
                list(area_rows[0].keys()),
                area_rows,
                self._configuration["long_hxltags"],
                folder,
                filename,
                resourcedata,
            )
        elif not group_rows:
            logger.error(f"{countryiso3} has no latest subnational data!")

        area_rows_wide = output["area_rows_wide_latest"]
        if area_rows_wide:
            filename = f"ipc_{countryiso3lower}_area_wide_latest.csv"
            resourcedata = {
                "name": filename,
                "description": "Latest IPC area data in wide form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
                list(area_rows_wide[0].keys()),
                area_rows_wide,
                self._configuration["wide_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        country_rows_wide = output["country_rows_wide"]
        if len(country_rows_wide) == 1:
            return dataset, showcase

        country_rows = output["country_rows"]
        filename = f"ipc_{countryiso3lower}_national_long.csv"
        resourcedata = {
            "name": filename,
            "description": "All IPC national data in long form with HXL tags",
        }
        success, results = dataset.generate_resource_from_iterable(
            list(country_rows[0].keys()),
            country_rows,
            self._configuration["long_hxltags"],
            folder,
            filename,
            resourcedata,
        )

        filename = f"ipc_{countryiso3lower}_national_wide.csv"
        resourcedata = {
            "name": filename,
            "description": "All IPC national data in wide form with HXL tags",
        }
        success, results = dataset.generate_resource_from_iterable(
            list(country_rows_wide[0].keys()),
            country_rows_wide,
            self._configuration["wide_hxltags"],
            folder,
            filename,
            resourcedata,
        )

        group_rows = output["group_rows"]
        if group_rows:
            filename = f"ipc_{countryiso3lower}_level1_long.csv"
            resourcedata = {
                "name": filename,
                "description": "All IPC level 1 data in long form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
                list(group_rows[0].keys()),
                group_rows,
                self._configuration["long_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        group_rows_wide = output["group_rows_wide"]
        if group_rows_wide:
            filename = f"ipc_{countryiso3lower}_level1_wide.csv"
            resourcedata = {
                "name": filename,
                "description": "All IPC level 1 data in wide form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
                list(group_rows_wide[0].keys()),
                group_rows_wide,
                self._configuration["wide_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        area_rows = output["area_rows"]
        if area_rows:
            filename = f"ipc_{countryiso3lower}_area_long.csv"
            resourcedata = {
                "name": filename,
                "description": "All IPC area data in long form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
                list(area_rows[0].keys()),
                area_rows,
                self._configuration["long_hxltags"],
                folder,
                filename,
                resourcedata,
            )
        elif not group_rows:
            logger.error(f"{countryiso3} has no subnational data!")

        area_rows_wide = output["area_rows_wide"]
        if area_rows_wide:
            filename = f"ipc_{countryiso3lower}_area_wide.csv"
            resourcedata = {
                "name": filename,
                "description": "All IPC area data in wide form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
                list(area_rows_wide[0].keys()),
                area_rows_wide,
                self._configuration["wide_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        return dataset, showcase
