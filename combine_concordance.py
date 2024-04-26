import glob
import os
import re
import shutil
import warnings

import fire
import pandas as pd

warnings.filterwarnings("ignore")


def output_filenames(root_folder: str, we_date: str) -> tuple:
    return (
        os.path.join(root_folder, f"concordance_we_{we_date}.csv"),
        os.path.join(root_folder, f"concordance_mms_we_{we_date}.csv"),
    )


def combine_concordance_files(fp: str, dt: str) -> None:
    concordance_output, concordance_mms_output = output_filenames(fp, dt)

    glob_path = os.path.join(fp, "*.xlsx")
    all_files = glob.glob(glob_path)

    all_files = [
        file for file in all_files if re.search(r"concordance", file, re.IGNORECASE)
    ]

    combined_df = pd.DataFrame()

    for file in all_files:
        print(file)
        # Read the .xlsx file
        if re.search(r"mms", file, re.IGNORECASE):
            pd.read_excel(file).to_csv(concordance_mms_output, index=False)
        else:
            df = pd.read_excel(file)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        shutil.move(file, os.path.join(fp, "completed", os.path.basename(file)))

    if len(combined_df) > 0:
        combined_df.to_csv(concordance_output, index=False)

    return concordance_output, concordance_mms_output


if __name__ == "__main__":
    fire.Fire(combine_concordance_files)
