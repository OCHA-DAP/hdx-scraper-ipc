#!/usr/bin/python
"""
Unit tests for InterAction.

"""
from datetime import datetime, timezone
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from ipc import IPC


class TestIPC:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yaml"),
        )
        UserAgent.set_global("test")
        Country.countriesdata(use_live=False)
        tags = (
            "hxl",
            "food security",
            "integrated food security phase classification-ipc",
        )
        Locations.set_validlocations(
            [
                {"name": x.lower(), "title": x.lower()}
                for x in ("world", "AFG", "AGO", "CAF", "ETH")
            ]
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            "name": "approved",
        }
        return Configuration.read()

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def input_folder(self, fixtures):
        return join(fixtures, "input")

    def test_generate_datasets_and_showcases(
        self, configuration, fixtures, input_folder
    ):
        with temp_dir(
            "test_ipc", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )

                def check_files(resources):
                    for resource in resources:
                        filename = resource["name"]
                        expected_path = join(fixtures, filename)
                        actual_path = join(folder, filename)
                        assert_files_same(expected_path, actual_path)

                state_dict = {"DEFAULT": parse_date("2017-01-01")}
                ipc = IPC(configuration, retriever, state_dict, ())
                countries = ipc.get_countries()
                assert countries == [
                    {"iso3": "AFG"},
                    {"iso3": "AGO"},
                    {"iso3": "CAF"},
                    {"iso3": "ETH"},
                ]

                output = ipc.get_country_data("AFG")
                dataset, showcase = ipc.generate_dataset_and_showcase(folder, output)
                assert dataset == {
                    "data_update_frequency": "-2",
                    "dataset_date": "[2017-05-01T00:00:00 TO 2023-10-31T23:59:59]",
                    "groups": [{"name": "afg"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "afghanistan-acute-food-insecurity-country-data",
                    "notes": "There is also a [global "
                    "dataset](https://stage.data-humdata-org.ahconu.org/dataset/global-acute-food-insecurity-country-data).",
                    "owner_org": "da501ffc-aadb-43f5-9d28-8fa572fd9ce0",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "integrated food security phase classification-ipc",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan: Acute Food Insecurity Country Data",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Latest IPC national data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_afg_national_long_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC area data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_afg_area_long_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC area data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_afg_area_wide_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC national data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_afg_national_long.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC national data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_afg_national_wide.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC level 1 data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_afg_level1_long.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC level 1 data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_afg_level1_wide.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC area data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_afg_area_long.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC area data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_afg_area_wide.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
                check_files(resources)
                assert showcase == {
                    "image_url": "https://www.ipcinfo.org/fileadmin/user_upload/ipcinfo/img/dashboard_thumbnail.jpg",
                    "name": "afghanistan-acute-food-insecurity-country-data-showcase",
                    "notes": "IPC-CH Dashboard",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "integrated food security phase classification-ipc",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan: Acute Food Insecurity Country Data showcase",
                    "url": "https://www.ipcinfo.org/ipc-country-analysis/en/?country=AFG",
                }
                ipc.ch_countries = ["AFG"]  # for testing purposes
                _, showcase = ipc.generate_dataset_and_showcase(folder, output)
                assert showcase == {
                    "image_url": "https://www.ipcinfo.org/fileadmin/user_upload/ipcinfo/img/dashboard_thumbnail.jpg",
                    "name": "afghanistan-acute-food-insecurity-country-data-showcase",
                    "notes": "IPC-CH Dashboard",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "integrated food security phase classification-ipc",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Afghanistan: Acute Food Insecurity Country Data showcase",
                    "url": "https://www.ipcinfo.org/ch/en/",
                }

                output = ipc.get_country_data("AGO")
                dataset, showcase = ipc.generate_dataset_and_showcase(folder, output)
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Latest IPC national data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_national_long_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC level 1 data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_level1_long_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC level 1 data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_level1_wide_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC area data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_area_long_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC area data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_area_wide_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC national data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_national_long.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC national data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_national_wide.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC level 1 data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_level1_long.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC level 1 data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_level1_wide.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC area data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_area_long.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC area data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_ago_area_wide.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
                check_files(resources)
                output = ipc.get_country_data("CAF")
                dataset, showcase = ipc.generate_dataset_and_showcase(folder, output)
                check_files(dataset.get_resources())
                output = ipc.get_country_data("ETH")
                dataset, showcase = ipc.generate_dataset_and_showcase(folder, output)
                check_files(dataset.get_resources())

                output = ipc.get_all_data()
                dataset, showcase = ipc.generate_dataset_and_showcase(folder, output)
                assert dataset == {
                    "data_update_frequency": "-2",
                    "dataset_date": "[2017-02-01T00:00:00 TO 2024-03-31T23:59:59]",
                    "groups": [{"name": "world"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "global-acute-food-insecurity-country-data",
                    "notes": "There are also [country "
                    "datasets](https://stage.data-humdata-org.ahconu.org/organization/da501ffc-aadb-43f5-9d28-8fa572fd9ce0)",
                    "owner_org": "da501ffc-aadb-43f5-9d28-8fa572fd9ce0",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "integrated food security phase classification-ipc",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Global: Acute Food Insecurity Country Data",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Latest IPC national data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_national_long_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC national data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_national_wide_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC level 1 data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_level1_long_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC level 1 data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_level1_wide_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC area data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_area_long_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Latest IPC area data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_area_wide_latest.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC national data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_national_long.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC national data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_national_wide.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC level 1 data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_level1_long.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC level 1 data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_level1_wide.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC area data in long form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_area_long.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "All IPC area data in wide form with HXL tags",
                        "format": "csv",
                        "name": "ipc_global_area_wide.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
                check_files(resources)
                assert showcase == {
                    "image_url": "https://www.ipcinfo.org/fileadmin/user_upload/ipcinfo/img/dashboard_thumbnail.jpg",
                    "name": "global-acute-food-insecurity-country-data-showcase",
                    "notes": "IPC-CH Dashboard",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "integrated food security phase classification-ipc",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Global: Acute Food Insecurity Country Data showcase",
                    "url": "https://www.ipcinfo.org/ipcinfo-website/ipc-dashboard/en/",
                }
                assert state_dict == {
                    "AFG": datetime(2023, 4, 1, 0, 0, tzinfo=timezone.utc),
                    "AGO": datetime(2021, 6, 1, 0, 0, tzinfo=timezone.utc),
                    "CAF": datetime(2023, 4, 1, 0, 0, tzinfo=timezone.utc),
                    "DEFAULT": datetime(2017, 1, 1, 0, 0, tzinfo=timezone.utc),
                    "END_DATE": datetime(2024, 3, 31, 0, 0, tzinfo=timezone.utc),
                    "ETH": datetime(2021, 5, 1, 0, 0, tzinfo=timezone.utc),
                    "START_DATE": datetime(2017, 2, 1, 0, 0, tzinfo=timezone.utc),
                }
