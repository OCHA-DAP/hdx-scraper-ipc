#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from copy import deepcopy
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.facades.infer_arguments import facade
from hdx.scraper.ipc.ipc import IPC
from hdx.scraper.ipc.ipc_hapi import HAPIOutput
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve
from hdx.utilities.state import State

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-ipc"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: IPC"


def main(
    save: bool = False,
    use_saved: bool = False,
    err_to_hdx: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        err_to_hdx (bool): Whether to write any errors to HDX metadata. Defaults to False.

    Returns:
        None
    """
    configuration = Configuration.read()
    with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
        with State(
            "analysis_dates.txt",
            State.dates_str_to_country_date_dict,
            State.country_date_dict_to_dates_str,
        ) as state:
            state_dict = deepcopy(state.get())
            with wheretostart_tempdir_batch(_USER_AGENT_LOOKUP) as info:
                folder = info["folder"]
                with Download(
                    extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
                    extra_params_lookup=_USER_AGENT_LOOKUP,
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

                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                            batch=info["batch"],
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
                        # create_dataset(
                        #     dataset,
                        #     showcase,
                        # )
                    if country_data_updated:
                        output = ipc.get_all_data()
                        dataset, showcase = ipc.generate_dataset_and_showcase(
                            folder, output
                        )
                        create_dataset(
                            dataset,
                            showcase,
                        )
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
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
