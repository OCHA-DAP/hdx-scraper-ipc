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
from slugify import slugify

logger = logging.getLogger(__name__)


class IPC:
    def __init__(self, configuration, retriever, last_run_date):
        self.configuration = configuration
        self.retriever = retriever
        self.last_run_date = last_run_date
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

    def generate_dataset_and_showcase(self, folder, countryiso3):
        countryiso2 = Country.get_iso2_from_iso3(countryiso3)
        url = f"{self.base_url}/population?country={countryiso2}"
        country_data = self.retriever.download_json(url)
        if not country_data:
            return None, None
        most_recent_analysis = country_data[0]

        def parse_date(datestring):
            date = datetime.strptime(datestring, "%b %Y")
            return date.replace(tzinfo=timezone.utc)

        analysis_date = parse_date(most_recent_analysis["analysis_date"])
        if analysis_date < self.last_run_date:
            return None, None
        country_rows = []
        country_rows_wide = []
        subnational_rows = []
        subnational_rows_wide = []

        def parse_date_range(date_range):
            start, end = date_range.split(" - ")
            startdate = parse_date(start)
            enddate = parse_date(end)
            startdatestr = startdate.date().isoformat()
            enddatestr = enddate.date().isoformat()
            return startdatestr, enddatestr

        def add_country_subnational_rows(base_row, location, rows, rows_wide):
            row_wide = deepcopy(base_row)
            for i, projection in enumerate(self.projections):
                projection_row = deepcopy(base_row)
                period_date = location.get(f"{projection}_period_dates")
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
                    affected = location.get(
                        f"{prefix}_population{projection_suffix}",
                        location[f"p3plus{projection_suffix}"],
                    )
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
                "country_iso3": countryiso3,
                "analysis_date": analysis["analysis_date"],
                "population": analysis["population"],
            }

        def add_country_rows(analysis):
            base_row = get_base_row(analysis)
            add_country_subnational_rows(
                base_row, analysis, rows=country_rows, rows_wide=country_rows_wide
            )

        def add_subnational_rows(analysis):
            base_row = get_base_row(analysis)
            ####
            add_country_subnational_rows(
                base_row, analysis, rows=subnational_rows, rows_wide=subnational_rows_wide
            )

        countryname = Country.get_country_name_from_iso3(countryiso3)

        title = f"{countryname}: Acute Food Insecurity Country Data"
        name = slugify(title).lower()
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset(
            {
                "name": name,
                "title": title,
                #                "notes": "",
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("da501ffc-aadb-43f5-9d28-8fa572fd9ce0")
        dataset.set_expected_update_frequency("As needed")
        dataset.set_subnational(True)
        dataset.add_country_location(countryiso3)
        tags = ("food security", "integrated food security phase classification-ipc")
        dataset.add_tags(tags)
        start_date = parse_date(country_data[-1]["analysis_date"])
        dataset.set_reference_period(start_date, analysis_date)
        #        global_dataset_url = global_dataset.get_hdx_url()
        #        notes = f"There is also a [global dataset]({global_dataset_url})."
        add_country_rows(most_recent_analysis)
        countryiso3lower = countryiso3.lower()
        filename = f"ipc_{countryiso3lower}_national_long_latest.csv"
        resourcedata = {
            "name": filename,
            "description": f"Latest IPC national data in long form with HXL tags",
        }
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
        # Won't do wide latest for country as just one row!

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

        if len(country_data) == 1:
            return dataset, showcase
        for analysis in country_data[1:]:
            add_country_rows(analysis)

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
        return dataset, showcase
