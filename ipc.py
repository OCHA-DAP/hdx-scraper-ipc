#!/usr/bin/python
"""
IPC:
----

Reads IPC data and creates datasets.

"""
import logging
from copy import deepcopy
from datetime import datetime, timezone

from dateutil.relativedelta import relativedelta
from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
)
from slugify import slugify

logger = logging.getLogger(__name__)


class IPC:
    def __init__(self, configuration, retriever, state, ch_countries):
        self.configuration = configuration
        self.retriever = retriever
        self.state = state
        self.default_start_date = state["DEFAULT"]
        self.base_url = configuration["base_url"]
        self.projection_names = ["Current", "Projection", "Second projection"]
        self.projection_suffixes = ["", "_projected", "_second_projected"]
        self.projections = ["current", "projected", "second_projected"]
        self.phasemapping = {
            "estimated": "all",
            "p3plus": "3+",
            "phase1": "1",
            "phase2": "2",
            "phase3": "3",
            "phase4": "4",
            "phase5": "5",
        }
        self.colnamemapping = {"estimated": "analyzed"}
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
        self.ch_countries = ch_countries

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
            update = False
        else:
            update = True
        self.state[countryiso3] = analysis_date
        time_period = {"start_date": default_enddate, "end_date": default_date}

        def parse_date_range(date_range):
            start, end = date_range.split(" - ")
            startdate = parse_date(start)
            if startdate < time_period["start_date"]:
                time_period["start_date"] = startdate
            enddate = parse_date(end)
            enddate = enddate + relativedelta(months=1, days=-1)
            if enddate > time_period["end_date"]:
                time_period["end_date"] = enddate
            startdatestr = startdate.date().isoformat()
            enddatestr = enddate.date().isoformat()
            return startdatestr, enddatestr

        def add_country_subnational_rows(
            base_row,
            location,
            rows,
            rows_wide,
            analysis=None,
        ):
            if analysis is None:
                analysis = location
            country_subnational_row = deepcopy(base_row)
            row_wide = deepcopy(country_subnational_row)
            for i, projection in enumerate(self.projections):
                projection_row = deepcopy(country_subnational_row)
                period_date = analysis.get(f"{projection}_period_dates")
                if period_date:
                    period_start, period_end = parse_date_range(period_date)
                else:
                    period_start = period_end = None
                projection_row["Validity period"] = projection
                projection_row["From"] = period_start
                projection_row["To"] = period_end
                projection_name = self.projection_names[i]
                projection_suffix = self.projection_suffixes[i]
                row_wide[f"{projection_name} from"] = period_start
                row_wide[f"{projection_name} to"] = period_end
                location[f"estimated_percentage{projection_suffix}"] = 1.0
                for prefix, phase in self.phasemapping.items():
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
                        row_wide[
                            f"Phase {phase} percentage {projection_name_l}"
                        ] = percentage
                    if affected is not None and period_date:
                        rows.append(row)

            rows_wide.append(row_wide)

        def get_base_row(analysis):
            return {
                "Date of analysis": analysis["analysis_date"],
                "Country": countryiso3,
                "Total country population": analysis.get("population"),
            }

        def add_country_rows(analysis, rows, rows_wide):
            base_row = get_base_row(analysis)
            add_country_subnational_rows(
                base_row,
                analysis,
                rows=rows,
                rows_wide=rows_wide,
            )

        def add_subnational_rows(
            analysis, group_rows, group_rows_wide, area_rows, area_rows_wide
        ):
            def process_areas(adm_row, adm):
                if adm["areas"] is None:
                    logger.error(
                        f"{countryiso3}: {analysis['title']} has blank \"areas\" field!"
                    )
                    return
                for area in adm["areas"]:
                    area_row = deepcopy(adm_row)
                    if "Level 1" not in area_row:
                        area_row["Level 1"] = None
                    area_row["Area"] = area["name"]
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
                    group_row["Level 1"] = group["name"]
                    add_country_subnational_rows(
                        group_row,
                        group,
                        rows=group_rows,
                        rows_wide=group_rows_wide,
                        analysis=analysis,
                    )
                    if "areas" in group:
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
        add_subnational_rows(
            most_recent_analysis,
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
            add_country_rows(analysis, country_rows, country_rows_wide)
            add_subnational_rows(
                analysis,
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
        start_date = time_period["start_date"]
        end_date = time_period["end_date"]
        output["start_date"] = start_date
        output["end_date"] = end_date
        if start_date < self.output["start_date"]:
            self.output["start_date"] = start_date
            self.state["START_DATE"] = start_date
        if end_date > self.output["end_date"]:
            self.output["end_date"] = end_date
            self.state["END_DATE"] = end_date
        if not update:
            return None
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
        dataset.set_time_period(output["start_date"], output["end_date"])

        filename = f"ipc_{countryiso3lower}_national_long_latest.csv"
        resourcedata = {
            "name": filename,
            "description": f"Latest IPC national data in long form with HXL tags",
        }
        country_rows = output["country_rows_latest"]
        if not country_rows:
            logger.warning(f"{filename} has no data!")
            return None, None
        success, results = dataset.generate_resource_from_iterable(
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
            success, results = dataset.generate_resource_from_iterable(
                list(country_rows_wide[0].keys()),
                country_rows_wide,
                self.configuration["wide_hxltags"],
                folder,
                filename,
                resourcedata,
            )

        if countryiso3lower == "global":
            showcase_url = "https://www.ipcinfo.org/ipcinfo-website/ipc-dashboard/en/"
        elif countryiso3 in self.ch_countries:
            showcase_url = self.configuration["ch_showcase_url"]
        else:
            showcase_url = self.configuration["showcase_url"]
            showcase_url = f"{showcase_url}{countryiso3}"
        showcase = Showcase(
            {
                "name": f"{name}-showcase",
                "title": f"{title} showcase",
                "notes": f"IPC-CH Dashboard",
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
                "description": f"Latest IPC level 1 data in long form with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterable(
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
            success, results = dataset.generate_resource_from_iterable(
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
        success, results = dataset.generate_resource_from_iterable(
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
        success, results = dataset.generate_resource_from_iterable(
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
        success, results = dataset.generate_resource_from_iterable(
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
        success, results = dataset.generate_resource_from_iterable(
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
            success, results = dataset.generate_resource_from_iterable(
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
            success, results = dataset.generate_resource_from_iterable(
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
        success, results = dataset.generate_resource_from_iterable(
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
        success, results = dataset.generate_resource_from_iterable(
            list(area_rows_wide[0].keys()),
            area_rows_wide,
            self.configuration["wide_hxltags"],
            folder,
            filename,
            resourcedata,
        )

        return dataset, showcase
