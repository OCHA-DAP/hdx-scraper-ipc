#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve
from hdx.utilities.state import State
from ipc import IPC, dict_to_str, str_to_dict

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-ipc"
updated_by_script = "HDX Scraper: IPC"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """

    configuration = Configuration.read()
    with State("analysis_dates.txt", str_to_dict, dict_to_str) as state:
        state_dict = state.get()
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            with Download(
                extra_params_yaml=join(expanduser("~"), ".extraparams.yml"),
                extra_params_lookup=lookup,
            ) as downloader:
                retriever = Retrieve(
                    downloader, folder, "saved_data", folder, save, use_saved
                )
                ipc = IPC(configuration, retriever, state_dict)
                countries = ipc.get_countries()
                logger.info(f"Number of countries: {len(countries)}")

                def create_dataset(
                    dataset,
                    showcase,
                ):
                    if not dataset:
                        return
                    notes = dataset.get("notes")
                    dataset.update_from_yaml()
                    if notes:
                        notes = f"{dataset['notes']}\n\n{notes}"
                    else:
                        notes = dataset["notes"]
                    # ensure markdown has line breaks
                    dataset["notes"] = notes.replace("\n", "  \n")

                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
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
                    create_dataset(
                        dataset,
                        showcase,
                    )
                if country_data_updated:
                    output = ipc.get_all_data()
                    dataset, showcase = ipc.generate_dataset_and_showcase(
                        folder, output
                    )
                    create_dataset(
                        dataset,
                        showcase,
                    )
                else:
                    logger.info("Nothing to update!")
        state.set(state_dict)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
