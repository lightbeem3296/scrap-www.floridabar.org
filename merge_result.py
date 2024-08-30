# drag & drop output folder here

from loguru import logger
import json
import os
import sys

import pandas as pd


def natural_sort_key(s):
    """
    Provides a sort key for strings that may or may not lead with an integer.
    """
    for i, c in enumerate(s):
        if not c.isdigit():
            break
    if not i:
        return s, 0
    else:
        return s[i:], int(s[:i])


def merge(src_dpath: str):
    try:
        if not os.path.isdir(src_dpath):
            print(f"[-] not directory: {src_dpath}")

        dst_dpath = os.path.dirname(src_dpath)
        src_dname = os.path.basename(src_dpath)
        dst_fpath = os.path.join(dst_dpath, f"{src_dname}")

        res0_df = pd.DataFrame(
            {
                "profile_name": [],
                "profile_url": [],
                "profile_image_url": [],
                "bar_number": [],
                "law_firm_name": [],
                "street_address_1": [],
                "street_address_2": [],
                "city": [],
                "state": [],
                "zip": [],
                "county": [],
                "admitted": [],
                "ten_year_disc": [],
                "law_school": [],
                "sections": [],
                "practice_areas": [],
                "firm_size": [],
                "firm_position": [],
                "languages": [],
                "firm_website": [],
                "office": [],
                "cell": [],
                "email": [],
            }
        )

        print(f"[*] merge: {src_dpath} > {dst_fpath}.*")
        for dpath, _, fnames in os.walk(src_dpath):
            sorted_fnames = sorted(fnames, key=natural_sort_key)
            for fname in sorted_fnames:
                if fname.lower().endswith(".json"):
                    fpath = os.path.join(dpath, fname)
                    print(f"[*] filepath: {fpath[len(src_dpath):]}")

                    with open(fpath, mode="r") as f:
                        info: dict[str, str] = json.load(f)
                        info["practice_areas"] = (
                            info["practice_areas"]
                            .replace("\r", "\n")
                            .replace("\n", ":")
                        )
                        info["sections"] = (
                            info["sections"]
                            .replace("\r", "\n")
                            .replace("\n", ":")
                        )
                        info["languages"] = (
                            info["languages"]
                            .replace("\r", "\n")
                            .replace("\n", ":")
                        )
                        res0_df = pd.concat(
                            [res0_df, pd.DataFrame([info])],
                            ignore_index=True,
                        )
        res0_df.to_excel(dst_fpath + ".xlsx", index=False)
    except Exception as ex:
        logger.exception(ex)


def main():
    try:
        if len(sys.argv) > 1:
            src_dlist = sys.argv[1:]
            for src_dir in src_dlist:
                print(f"[*] src_dir: {src_dir}")
            for src_dir in src_dlist:
                merge(src_dir)
    except Exception as ex:
        logger.exception(ex)


if __name__ == "__main__":
    main()
