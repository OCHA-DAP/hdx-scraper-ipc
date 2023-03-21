#!/usr/bin/python
"""
IPC:
----

Reads IPC data and creates datasets.

"""
import logging
from copy import deepcopy
from datetime import datetime, timezone

from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    iso_string_from_datetime,
    parse_date,
)
from slugify import slugify

logger = logging.getLogger(__name__)


def str_to_dict(string: str):
    result = {}
    for keyvalue in string.split(","):
        key, value = keyvalue.split("=")
        result[key] = parse_date(value)
    return result


def dict_to_str(dictionary: dict):
    strlist = []
    for key, value in dictionary.items():
        valstr = iso_string_from_datetime(value)
        strlist.append(f"{key}={valstr}")
    return ",".join(strlist)


class IPC:
    def __init__(self, configuration, retriever, state):
        self.configuration = configuration
        self.retriever = retriever
        self.state = state
        self.default_start_date = state["DEFAULT"]
        self.base_url = configuration["base_url"]
        self.projection_suffixes = ["", "_projected", "_second_projected"]
        self.projections = ["current", "projected", "second_projected"]
        self.mapping = {
            "estimated": "all",
            "p3plus": "P3+",
            "phase1": "1",
            "phase2": "2",
            "phase3": "3",
            "phase4": "4",
            "phase5": "5",
        }
        self.prefixmapping = {"estimated": "affected"}
        self.output = {
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
        self.global_dataset_url = temp_dataset.get_hdx_url()

    def get_dataset_title_name(self, countryname):
        title = f"{countryname}: Acute Food Insecurity Country Data"
        name = slugify(title).lower()
        return name, title

    def get_countries(self):
        countryisos = set()
        json = self.retriever.download_json(f"{self.base_url}/analyses?type=A")
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

    def get_country_data(self, countryiso3):
        countryiso2 = Country.get_iso2_from_iso3(countryiso3)
        url = f"{self.base_url}/population?country={countryiso2}"
        country_data = self.retriever.download_json(url)
        if not country_data:
            return None
        most_recent_analysis = country_data[0]

        def parse_date(datestring):
            date = datetime.strptime(datestring, "%b %Y")
            return date.replace(tzinfo=timezone.utc)

        analysis_date = parse_date(most_recent_analysis["analysis_date"])
        if analysis_date <= self.state.get(countryiso3, self.default_start_date):
            return None
        self.state[countryiso3] = analysis_date

        def parse_date_range(date_range):
            start, end = date_range.split(" - ")
            startdate = parse_date(start)
            enddate = parse_date(end)
            startdatestr = startdate.date().isoformat()
            enddatestr = enddate.date().isoformat()
            return startdatestr, enddatestr

        def add_country_subnational_rows(
            base_row, location, rows, rows_wide, analysis=None
        ):
            country_subnational_row = deepcopy(base_row)
            country_subnational_row["population"] = location.get("population")
            row_wide = deepcopy(country_subnational_row)
            for i, projection in enumerate(self.projections):
                projection_row = deepcopy(country_subnational_row)
                period_date = location.get(f"{projection}_period_dates")
                if not period_date:
                    if not analysis:
                        continue
                    period_date = analysis.get(f"{projection}_period_dates")
                    if not period_date:
                        continue
                period_start, period_end = parse_date_range(period_date)
                projection_row["projection"] = projection
                projection_row["period_start"] = period_start
                projection_row["period_end"] = period_end
                projection_suffix = self.projection_suffixes[i]
                row_wide[f"period_start{projection_suffix}"] = period_start
                row_wide[f"period_end{projection_suffix}"] = period_end
                location[f"estimated_percentage{projection_suffix}"] = 1.0
                for prefix, phase in self.mapping.items():
                    row = deepcopy(projection_row)
                    if phase == "P3+":
                        key = f"p3plus{projection_suffix}"
                    else:
                        key = f"{prefix}_population{projection_suffix}"
                    if key in location:
                        affected = location[key]
                    else:
                        continue
                    row["phase"] = phase
                    row["number_affected"] = affected
                    prefixmapping = self.prefixmapping.get(prefix, prefix)
                    row_wide[
                        f"{prefixmapping}_population{projection_suffix}"
                    ] = affected
                    percentage = location[f"{prefix}_percentage{projection_suffix}"]
                    row["percentage_affected"] = percentage
                    if prefix != "estimated":
                        row_wide[
                            f"{prefixmapping}_percentage{projection_suffix}"
                        ] = percentage
                    rows.append(row)

            rows_wide.append(row_wide)

        def get_base_row(analysis):
            return {
                "analysis_date": analysis["analysis_date"],
                "country_iso3": countryiso3,
            }

        def add_country_rows(analysis, rows, rows_wide):
            base_row = get_base_row(analysis)
            add_country_subnational_rows(
                base_row, analysis, rows=rows, rows_wide=rows_wide
            )

        def add_subnational_rows(
            analysis, is_2_level, group_rows, group_rows_wide, area_rows, area_rows_wide
        ):
            def process_areas(adm_row, adm):
                for area in adm["areas"]:
                    area_row = deepcopy(adm_row)
                    if is_2_level and "level1_name" not in area_row:
                        area_row["level1_name"] = None
                    area_row["area"] = area["name"]
                    add_country_subnational_rows(
                        area_row,
                        area,
                        rows=area_rows,
                        rows_wide=area_rows_wide,
                        analysis=analysis,
                    )

            base_row = get_base_row(analysis)
            groups = analysis.get("groups")
            if groups:
                for group in analysis["groups"]:
                    group_row = deepcopy(base_row)
                    group_row["level1_name"] = group["name"]
                    add_country_subnational_rows(
                        group_row,
                        group,
                        rows=group_rows,
                        rows_wide=group_rows_wide,
                        analysis=analysis,
                    )
                    process_areas(group_row, group)
            else:
                process_areas(base_row, analysis)

        output = {"countryiso3": countryiso3}
        country_rows = output["country_rows_latest"] = []
        country_rows_wide = output["country_rows_wide_latest"] = []
        group_rows = output["group_rows_latest"] = []
        group_rows_wide = output["group_rows_wide_latest"] = []
        area_rows = output["area_rows_latest"] = []
        area_rows_wide = output["area_rows_wide_latest"] = []
        add_country_rows(most_recent_analysis, country_rows, country_rows_wide)
        if "groups" in most_recent_analysis:
            is_2_level = True
        else:
            is_2_level = False
        add_subnational_rows(
            most_recent_analysis,
            is_2_level,
            group_rows,
            group_rows_wide,
            area_rows,
            area_rows_wide,
        )
        self.output["country_rows_latest"].extend(country_rows)
        self.output["country_rows_wide_latest"].extend(country_rows_wide)
        self.output["group_rows_latest"].extend(group_rows)
        self.output["group_rows_wide_latest"].extend(group_rows_wide)
        self.output["area_rows_latest"].extend(area_rows)
        self.output["area_rows_wide_latest"].extend(area_rows_wide)

        country_rows = output["country_rows"] = []
        country_rows_wide = output["country_rows_wide"] = []
        group_rows = output["group_rows"] = []
        group_rows_wide = output["group_rows_wide"] = []
        area_rows = output["area_rows"] = []
        area_rows_wide = output["area_rows_wide"] = []
        for analysis in country_data:
            if "groups" in analysis:
                is_2_level = True

        for analysis in country_data:
            add_country_rows(analysis, country_rows, country_rows_wide)
            add_subnational_rows(
                analysis,
                is_2_level,
                group_rows,
                group_rows_wide,
                area_rows,
                area_rows_wide,
            )
        self.output["country_rows"].extend(country_rows)
        self.output["country_rows_wide"].extend(country_rows_wide)
        self.output["group_rows"].extend(group_rows)
        self.output["group_rows_wide"].extend(group_rows_wide)
        self.output["area_rows"].extend(area_rows)
        self.output["area_rows_wide"].extend(area_rows_wide)
        start_date = parse_date(country_data[-1]["analysis_date"])
        output["start_date"] = start_date
        output["end_date"] = analysis_date
        if start_date < self.output["start_date"]:
            self.output["start_date"] = start_date
            self.state["START_DATE"] = start_date
        if analysis_date > self.output["end_date"]:
            self.output["end_date"] = analysis_date
            self.state["END_DATE"] = analysis_date
        return output

    def get_all_data(self):
        return self.output

    def generate_dataset_and_showcase(self, folder, output):
        if not output:
            return None, None
        countryiso3 = output.get("countryiso3")
        if countryiso3:
            countryname = Country.get_country_name_from_iso3(countryiso3)
            notes = f"There is also a [global dataset]({self.global_dataset_url})."
        else:
            if not output["country_rows_latest"]:
                return None, None
            countryname = "Global"
            notes = f"There are also [country datasets]({self.configuration.get_hdx_site_url()}/organization/da501ffc-aadb-43f5-9d28-8fa572fd9ce0)"
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
        dataset.set_reference_period(output["start_date"], output["end_date"])

        filename = f"ipc_{countryiso3lower}_national_long_latest.csv"
        resourcedata = {
            "name": filename,
            "description": f"Latest IPC national data in long form with HXL tags",
        }
        country_rows = output["country_rows_latest"]
        if not country_rows:
            logger.warning(f"{filename} has no data!")
            return None, None
        success, results = dataset.generate_resource_from_iterator(
            list(country_rows[0].keys()),
            country_rows,
            self.configuration["long_hxltags"],
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
                "description": f"Latest IPC national data in wide form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterator(
                list(country_rows_wide[0].keys()),
                country_rows_wide,
                self.configuration["wide_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        showcase = Showcase(
            {
                "name": f"{name}-showcase",
                "title": f"{title} showcase",
                "notes": f"IPC-CH Dashboard",
                "url": "https://www.ipcinfo.org/ipcinfo-website/ipc-dashboard/en/",
                "image_url": "https://www.ipcinfo.org/fileadmin/user_upload/ipcinfo/img/dashboard_thumbnail.jpg",
            }
        )
        showcase.add_tags(tags)
        group_rows = output["group_rows_latest"]
        if group_rows:
            filename = f"ipc_{countryiso3lower}_level1_long_latest.csv"
            resourcedata = {
                "name": filename,
                "description": f"Latest IPC level 1 data in long form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterator(
                list(group_rows[0].keys()),
                group_rows,
                self.configuration["long_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        group_rows_wide = output["group_rows_wide_latest"]
        if group_rows_wide:
            filename = f"ipc_{countryiso3lower}_level1_wide_latest.csv"
            resourcedata = {
                "name": filename,
                "description": f"Latest IPC level 1 data in wide form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterator(
                list(group_rows_wide[0].keys()),
                group_rows_wide,
                self.configuration["wide_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        area_rows = output["area_rows_latest"]
        filename = f"ipc_{countryiso3lower}_area_long_latest.csv"
        resourcedata = {
            "name": filename,
            "description": f"Latest IPC area data in long form with HXL tags",
        }
        success, results = dataset.generate_resource_from_iterator(
            list(area_rows[0].keys()),
            area_rows,
            self.configuration["long_hxltags"],
            folder,
            filename,
            resourcedata,
        )

        area_rows_wide = output["area_rows_wide_latest"]
        filename = f"ipc_{countryiso3lower}_area_wide_latest.csv"
        resourcedata = {
            "name": filename,
            "description": f"Latest IPC area data in wide form with HXL tags",
        }
        success, results = dataset.generate_resource_from_iterator(
            list(area_rows_wide[0].keys()),
            area_rows_wide,
            self.configuration["wide_hxltags"],
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
            "description": f"All IPC national data in long form with HXL tags",
        }
        success, results = dataset.generate_resource_from_iterator(
            list(country_rows[0].keys()),
            country_rows,
            self.configuration["long_hxltags"],
            folder,
            filename,
            resourcedata,
        )

        filename = f"ipc_{countryiso3lower}_national_wide.csv"
        resourcedata = {
            "name": filename,
            "description": f"All IPC national data in wide form with HXL tags",
        }
        success, results = dataset.generate_resource_from_iterator(
            list(country_rows_wide[0].keys()),
            country_rows_wide,
            self.configuration["wide_hxltags"],
            folder,
            filename,
            resourcedata,
        )

        group_rows = output["group_rows"]
        if group_rows:
            filename = f"ipc_{countryiso3lower}_level1_long.csv"
            resourcedata = {
                "name": filename,
                "description": f"All IPC level 1 data in long form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterator(
                list(group_rows[0].keys()),
                group_rows,
                self.configuration["long_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        group_rows_wide = output["group_rows_wide"]
        if group_rows_wide:
            filename = f"ipc_{countryiso3lower}_level1_wide.csv"
            resourcedata = {
                "name": filename,
                "description": f"All IPC level 1 data in wide form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterator(
                list(group_rows_wide[0].keys()),
                group_rows_wide,
                self.configuration["wide_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        area_rows = output["area_rows"]
        filename = f"ipc_{countryiso3lower}_area_long.csv"
        resourcedata = {
            "name": filename,
            "description": f"All IPC area data in long form with HXL tags",
        }
        success, results = dataset.generate_resource_from_iterator(
            list(area_rows[0].keys()),
            area_rows,
            self.configuration["long_hxltags"],
            folder,
            filename,
            resourcedata,
        )

        area_rows_wide = output["area_rows_wide"]
        filename = f"ipc_{countryiso3lower}_area_wide.csv"
        resourcedata = {
            "name": filename,
            "description": f"All IPC area data in wide form with HXL tags",
        }
        success, results = dataset.generate_resource_from_iterator(
            list(area_rows_wide[0].keys()),
            area_rows_wide,
            self.configuration["wide_hxltags"],
            folder,
            filename,
            resourcedata,
        )

        return dataset, showcase
