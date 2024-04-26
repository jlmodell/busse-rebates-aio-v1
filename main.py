import os
import shutil

import fire

from maps import current_file_maps, ingest, transform


def main(
    distributor: str,
    month: str | int,
    year: str | int,
    file_path: str,
    day: str | int = 1,
):
    distributor = distributor.lower()

    base_path = os.path.dirname(file_path)
    print(f"Base path: {base_path}")
    # create completed folder if it doesn't exist
    completed_folder = os.path.join(base_path, "completed")
    if not os.path.exists(completed_folder):
        os.mkdir(completed_folder)

    if distributor not in current_file_maps:
        for key in current_file_maps.keys():
            print(key)
        print()

        raise ValueError(f"{distributor} is not a valid distributor")

    if (
        distributor == "concordance"
        or distributor == "concordance_mms"
        and not file_path.endswith(".xlsx")
        and not file_path.endswith(".xls")
        and not file_path.endswith(".csv")
    ):
        from combine_concordance import combine_concordance_files

        concordance, concordance_mms = combine_concordance_files(
            file_path, f"{year:04}{month:02}{day:02}"
        )
        concordance_fp = os.path.join(file_path, concordance)
        concordance_mms_fp = os.path.join(file_path, concordance_mms)

        print(f"Concordance file: {concordance_fp}")
        tracings_df = ingest(
            "concordance",
            f"{month:02}",
            f"{year:04}",
            concordance_fp,
            current_file_maps["concordance"]["float_cols"],
            current_file_maps["concordance"]["date_col"],
            current_file_maps["concordance"]["requires_cost_calc"],
            current_file_maps["concordance"]["col_map"],
        )
        transform("concordance", int(month), int(year), tracings_df)

        # move the file to the completed folder
        shutil.move(
            concordance_fp,
            os.path.join(completed_folder, os.path.basename(concordance_fp)),
        )

        print(f"Concordance MMS file: {concordance_mms_fp}")
        tracings_df = ingest(
            "concordance_mms",
            f"{month:02}",
            f"{year:04}",
            concordance_mms_fp,
            current_file_maps["concordance_mms"]["float_cols"],
            current_file_maps["concordance_mms"]["date_col"],
            current_file_maps["concordance_mms"]["requires_cost_calc"],
            current_file_maps["concordance_mms"]["col_map"],
        )
        transform("concordance_mms", int(month), int(year), tracings_df)

        # move the file to the completed folder
        shutil.move(
            concordance_mms_fp,
            os.path.join(completed_folder, os.path.basename(concordance_mms_fp)),
        )

    else:
        print(f"File: {file_path}")
        tracings_df = ingest(
            distributor,
            f"{month:02}",
            f"{year:04}",
            file_path,
            current_file_maps[distributor]["float_cols"],
            current_file_maps[distributor]["date_col"],
            current_file_maps[distributor]["requires_cost_calc"],
            current_file_maps[distributor]["col_map"],
        )
        transform(distributor, int(month), int(year), tracings_df)

        shutil.move(
            file_path, os.path.join(completed_folder, os.path.basename(file_path))
        )


if __name__ == "__main__":
    fire.Fire(main)
