import argparse
import json
import time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from loguru import logger
from playwright.sync_api import sync_playwright

CUR_DIR = Path(__file__).parent
OUT_DIR = CUR_DIR / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.floridabar.org"
PAGE = None

DONE_MARKER_NAME = "done"
PAGE_SIZE = 10

KEYS = [
    "profile_name",
    "profile_url",
    "profile_image_url",
    "bar_number",
    "law_firm_name",
    "street_address_1",
    "street_address_2",
    "city",
    "state",
    "zip",
    "county",
    "admitted",
    "ten_year_disc",
    "law_school",
    "sections",
    "practice_areas",
    "firm_size",
    "firm_position",
    "languages",
    "firm_website",
    "office",
    "cell",
    "email",
]

SEARCH_LIST = [
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Jacksonville&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Miami&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Tampa&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Orlando&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=st%20petersburg&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=port%20saint%20lucie&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=cape%20coral&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Hialeah&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Tallahassee&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Fort%20Lauderdale&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=pembroke%20pines&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=hollywood&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=gainesville&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Miramar&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=palm%20bay&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Coral%20Springs&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=West%20Palm%20Beach&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=lakeland&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Spring%20Hill&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=clearwater&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=brandon&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=pompano%20beach&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=miami%20gardens&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=davie&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=riverview&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=palm%20coast&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=boca%20raton&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=fort%20myers&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=sunrise&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=deerfield%20beach&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=melbourne&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=daytona%20beach&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=largo&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=homestead&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=Kissimmee&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=boynton%20beach&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=miami%20beach&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=doral&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
    "https://www.floridabar.org/directories/find-mbr/?locType=C&locValue=the%20villages&sdx=N&eligible=Y&deceased=N&pageNumber=1&pageSize=10",
]


def mark_as_done(dir_path: Path) -> None:
    try:
        with open(dir_path / DONE_MARKER_NAME, "w") as f:
            f.write("done")
    except Exception as ex:
        logger.exception(ex)


def is_done(dir_path: Path) -> bool:
    return (dir_path / DONE_MARKER_NAME).is_file()


def normalize_str(text: str) -> str:
    ret = text.replace("\r", "\n").replace("\t", " ")

    # remove double space
    while "  " in ret:
        ret = ret.replace("  ", " ")

    # remove left space of the middle line
    while "\n " in ret:
        ret = ret.replace("\n ", "\n")

    # remove right space of the middle line
    while " \n" in ret:
        ret = ret.replace(" \n", "\n")

    # remove empty line
    while "\n\n" in ret:
        ret = ret.replace("\n\n", "\n")

    # remove useless new line
    while ",\n" in ret:
        ret = ret.replace(",\n", ", ")

    return ret


def fetch(url: str, referer: str = "", delay: float = 3.0) -> Optional[BeautifulSoup]:
    ret = None

    if not url.startswith(BASE_URL):
        url = BASE_URL + url

    if not referer.startswith(BASE_URL):
        referer = BASE_URL + referer

    while True:
        try:
            time.sleep(delay)

            PAGE.goto(url=url, referer=referer, wait_until="networkidle")
            if PAGE.query_selector("a.logoHeader") is not None:
                ret = BeautifulSoup(PAGE.content(), "html.parser")
                break
            else:
                # Clear cookies
                PAGE.context.clear_cookies()

                # Clear local storage and session storage
                PAGE.evaluate("localStorage.clear()")
                PAGE.evaluate("sessionStorage.clear()")
                time.sleep(10)
        except Exception as ex:
            logger.exception(ex)
    return ret


def work_profile(
    page_dir: Path, page_link: str, profile_index: int, profile_link: str
) -> None:
    # check if already done
    file_path = page_dir / (str(profile_index) + ".json")
    if file_path.is_file():
        with file_path.open("r") as file:
            content: dict[str, str] = json.load(file)
            error = False
            for key in KEYS:
                if key not in content:
                    error = True
                    break
            if not error:
                logger.info("already done")
                return

    # fetch profile content
    profile_content = fetch(profile_link, referer=page_link)

    profile: dict[str, str] = {}
    for key in KEYS:
        profile[key] = ""

    # Profile Name
    profile_name_elem = profile_content.select_one("h1.full")
    if profile_name_elem is not None:
        profile["profile_name"] = normalize_str(profile_name_elem.text.strip())

    # Profile Image URL
    img_elem = profile_content.select_one("div.profile-photo>div>img")
    if img_elem is not None:
        profile["profile_image_url"] = img_elem.attrs["src"]

    row_elems = profile_content.select("div.row")
    for row_elem in row_elems:
        cell_elems = row_elem.select("div.col-xs-12")
        if len(cell_elems) != 2:
            continue

        name_elem = cell_elems[0]
        name = name_elem.text.strip()
        value_elem = cell_elems[1]
        value = normalize_str(value_elem.text.strip())

        if name == "Mail Address:":
            paragraph_elems = value_elem.select("p")
            for paragraph_elem in paragraph_elems:
                a_elem = paragraph_elem.select_one("a")
                paragraph_text = paragraph_elem.text.strip()

                # Office
                if paragraph_text.startswith("Office:"):
                    profile["office"] = a_elem.text.strip()

                # Cell
                elif paragraph_text.startswith("Cell:"):
                    profile["cell"] = a_elem.text.strip()

                # Fax
                elif paragraph_text.startswith("Fax:"):
                    pass

                else:
                    html = paragraph_elem.decode_contents()
                    lines = html.split("<br/>")
                    # Street Address 1
                    profile["street_address_1"] = lines[1].strip()

                    # Street Address 2
                    if len(lines) >= 4:
                        profile["street_address_2"] = lines[2].strip()

                    line = lines[-1]
                    # City
                    profile["city"] = line.split(",")[0].strip()

                    # State
                    profile["state"] = line.split(",")[1].strip().split(" ")[0].strip()

                    # Zip
                    profile["zip"] = line.split(",")[1].strip().split(" ")[1].strip()

        # Profile URL
        profile["profile_url"] = profile_link

        # Bar Number
        if name == "Bar Number:":
            profile["bar_number"] = value

        # Law Firm Name
        if name == "Firm:":
            profile["law_firm_name"] = value

        # County
        if name == "County:":
            profile["county"] = value

        # Admitted
        if name == "Admitted:":
            profile["admitted"] = value

        # 10-year disc.
        if name.startswith("10-Year Discipline"):
            profile["ten_year_disc"] = value

        # Law School
        if name == "Law School:":
            profile["law_school"] = value

        # Sections
        if name == "Sections:":
            profile["sections"] = value

        # Practice Areas
        if name == "Practice Areas:":
            profile["practice_areas"] = value

        # Firm Size
        if name == "Firm Size:":
            profile["firm_size"] = value

        # Firm Position
        if name == "Firm Position:":
            profile["firm_position"] = value

        # Languages
        if name == "Languages:":
            profile["languages"] = value

        # Firm Website
        if name == "Firm Website:":
            profile["firm_website"] = value

        # Email
        if name == "Email:":
            profile["email"] = value

    # save profile information
    with file_path.open("w") as file:
        json.dump(profile, file, indent=2, default=str)


def work_page(loc_dir: Path, page_index: int, page_link: str) -> BeautifulSoup:
    page_content = fetch(page_link)

    page_dir = loc_dir / str(page_index)
    page_dir.mkdir(parents=True, exist_ok=True)

    if is_done(page_dir):
        logger.info("already done")
    else:
        items = page_content.select("li.profile-compact")
        for i, item in enumerate(items):
            profile_link_elem = item.select_one("a")
            profile_link = profile_link_elem.attrs["href"]

            logger.info(f"profile: {i}/{len(items)} >> {profile_link}")

            work_profile(
                page_dir=page_dir,
                page_link=page_link,
                profile_index=i,
                profile_link=profile_link,
            )

        mark_as_done(page_dir)

    return page_content


def work_location_link(loc_index: int, loc_link: str) -> None:
    parsed_url = urlparse(loc_link)
    query_params = parse_qs(parsed_url.query)
    loc_value = query_params.get("locValue", [None])[0]
    logger.info(f"location: {loc_value}, search_link: {loc_link}")
    loc_dir = OUT_DIR / (str(loc_index) + "." + loc_value)
    loc_dir.mkdir(parents=True, exist_ok=True)

    if is_done(loc_dir):
        return

    search_result = fetch(loc_link)
    if search_result is not None:
        res_msg_elem = search_result.select_one("p.result-message")
        if res_msg_elem is not None:
            total_items = int(res_msg_elem.text.strip().split(" ")[-2].replace(",", ""))
            total_pages = (total_items + PAGE_SIZE - 1) // PAGE_SIZE
            logger.info(f"total_items: {total_items}, total_pages: {total_pages}")

            page_link = loc_link

            for page_index in range(total_pages):
                logger.info(f"page_index: {page_index} / {total_pages}")

                page_content = work_page(
                    loc_dir=loc_dir,
                    page_index=page_index,
                    page_link=page_link,
                )

                # go to next page
                if page_index < total_pages - 1:
                    next_page_link_item = page_content.select_one(
                        "a[title='next page']"
                    )
                    next_page_link = next_page_link_item.attrs["href"]
                    logger.info(f"next_page_link: {next_page_link}")

                    page_link = next_page_link

            mark_as_done(loc_dir)
        else:
            logger.error("failed to find result message element")
    else:
        logger.error("failed to fetch search result")


def test():
    work_profile(
        OUT_DIR,
        0,
        "https://www.floridabar.org/directories/find-mbr/profile/?num=349623",
    )
    work_profile(
        OUT_DIR,
        1,
        "https://www.floridabar.org/directories/find-mbr/profile/?num=91287",
    )


def main():
    global PAGE

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--index",
        type=int,
        required=False,
        default=0,
        help="Search URL index: 0~38",
    )
    args = parser.parse_args()

    index = int(args.index)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        PAGE = browser.new_page()
        work_location_link(loc_index=index, loc_link=SEARCH_LIST[index])
        browser.close()
        logger.info("all done")


if __name__ == "__main__":
    main()
    # test()
