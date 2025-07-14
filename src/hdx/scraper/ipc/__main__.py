#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from copy import deepcopy
from os import getenv
from os.path import expanduser, join
from typing import Optional

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.api.utilities.hdx_state import HDXState
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.ipc._version import __version__
from hdx.scraper.ipc.ipc import IPC
from hdx.scraper.ipc.ipc_hapi import HAPIOutput
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-ipc"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: IPC"


def main(
    save: bool = False,
    use_saved: bool = False,
    err_to_hdx: Optional[str] = None,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        err_to_hdx (Optional[str]): Whether to write errors to HDX metadata. Defaults to None.

    Returns:
        None
    """
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access(
        "da501ffc-aadb-43f5-9d28-8fa572fd9ce0", configuration=configuration
    )
    with wheretostart_tempdir_batch(_LOOKUP) as info:
        folder = info["folder"]
        with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
            with HDXState(
                "pipeline-state-ipc",
                folder,
                HDXState.dates_str_to_country_date_dict,
                HDXState.country_date_dict_to_dates_str,
                configuration,
            ) as state:
                state_dict = deepcopy(state.get())
                ipc_key = getenv("IPC_KEY")
                if ipc_key:
                    extra_params_dict = {"key": ipc_key}
                else:
                    extra_params_dict = None
                with Download(
                    extra_params_dict=extra_params_dict,
                    extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
                    extra_params_lookup=_LOOKUP,
                ) as downloader:
                    _, iterator = downloader.get_tabular_rows(
                        script_dir_plus_file(join("config", "ch_countries.csv"), main),
                        dict_form=True,
                    )
                    ch_countries = [row["ISO_3"] for row in iterator]
                    retriever = Retrieve(
                        downloader, folder, "saved_data", folder, save, use_saved
                    )
                    ipc = IPC(configuration, retriever, state_dict, ch_countries)
                    countries = ipc.get_countries()
                    logger.info(f"Number of countries: {len(countries)}")

                    def create_dataset(
                        dataset,
                        showcase,
                        is_country=True,
                    ):
                        if not dataset:
                            return
                        notes = dataset.get("notes")
                        dataset.update_from_yaml(
                            path=script_dir_plus_file(
                                join("config", "hdx_dataset_static.yaml"), main
                            )
                        )
                        if notes:
                            notes = f"{dataset['notes']}\n\n{notes}"
                        else:
                            notes = dataset["notes"]
                        # ensure markdown has line breaks
                        dataset["notes"] = notes.replace("\n", "  \n")

                        resource_order = [x["name"] for x in dataset.get_resources()]

                        dataset.get_resource().disable_dataset_preview()
                        dataset.preview_off()
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                            batch=info["batch"],
                        )

                        if (
                            is_country
                            and dataset.get_resource().get_format() != "geojson"
                        ):
                            resource_ids = {}
                            for resource in dataset.get_resources():
                                resource_ids[resource["name"]] = resource["id"]
                            dataset.reorder_resources(
                                [resource_ids[x] for x in resource_order]
                            )

                        if showcase:
                            showcase.create_in_hdx()
                            showcase.add_dataset(dataset)

                    country_data_updated = False
                    for _, country in progress_storing_folder(info, countries, "iso3"):
                        countryiso = country["iso3"]
                        output = ipc.get_country_data(countryiso)
                        if output:
                            country_data_updated = True
                        dataset, showcase = ipc.generate_dataset_and_showcase(
                            folder, output
                        )
                        create_dataset(
                            dataset,
                            showcase,
                        )
                    if country_data_updated:
                        output = ipc.get_all_data()
                        dataset, showcase = ipc.generate_dataset_and_showcase(
                            folder, output
                        )
                        create_dataset(dataset, showcase, is_country=False)
                        hapi_output = HAPIOutput(
                            configuration, retriever, folder, error_handler, output
                        )
                        dataset = hapi_output.generate_dataset()
                        dataset.update_from_yaml(
                            path=script_dir_plus_file(
                                join(
                                    "config",
                                    "hdx_hapi_dataset_static.yaml",
                                ),
                                main,
                            )
                        )
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            match_resource_order=False,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                        )
                    else:
                        logger.info("Nothing to update!")
                state.set(state_dict)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
