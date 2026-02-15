#!/usr/bin/env python3
"""
download_wiki_plates.py

Downloads standard license plate images for European countries from the
Wikipedia article "European vehicle registration plate". The article
contains curated wikitables mapping countries to specific Wikimedia
Commons images.  This script parses those tables via the MediaWiki API,
resolves download URLs via the Commons API, and saves images + metadata.

Output:
    dataset/plates/europe/{CODE}_wiki.{ext}
    dataset/metadata/wiki_plates.json
    dataset/metadata/wiki_download_log.json

Usage:
    python scripts/download_wiki_plates.py              # Full run
    python scripts/download_wiki_plates.py --dry-run    # Parse only, no downloads
    python scripts/download_wiki_plates.py --country DE  # Single country
"""

import argparse
import json
import re
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
COMMONS_API_URL = "https://commons.wikimedia.org/w/api.php"
USER_AGENT = "PlateSpotter/1.0 (https://github.com/platespotter; contact@platespotter.dev)"
REQUEST_DELAY = 1.0
ARTICLE_TITLE = "European_vehicle_registration_plate"

# Map the vehicle registration code used on the wiki page to ISO alpha-2
# codes used in this project.  Codes that already match are omitted.
WIKI_CODE_TO_ISO = {
    "A":   "AT",
    "B":   "BE",
    "BIH": "BA",
    "BG":  "BG",
    "HR":  "HR",
    "CY":  "CY",
    "CZ":  "CZ",
    "DK":  "DK",
    "EST": "EE",
    "FIN": "FI",
    "F":   "FR",
    "D":   "DE",
    "GR":  "GR",
    "H":   "HU",
    "IS":  "IS",
    "IRL": "IE",
    "I":   "IT",
    "LV":  "LV",
    "FL":  "LI",
    "LT":  "LT",
    "L":   "LU",
    "M":   "MT",
    "MC":  "MC",
    "MNE": "ME",
    "NL":  "NL",
    "NMK": "MK",
    "N":   "NO",
    "PL":  "PL",
    "P":   "PT",
    "RO":  "RO",
    "RSM": "SM",
    "SRB": "RS",
    "SK":  "SK",
    "SLO": "SI",
    "E":   "ES",
    "S":   "SE",
    "CH":  "CH",
    "UA":  "UA",
    "UK":  "GB",
    "V":   "VA",
    "AND": "AD",
    "AM":  "AM",
    "AZ":  "AZ",
    "GE":  "GE",
    "RUS": "RU",
    "TR":  "TR",
    # Dependent territories
    "AX":  "AX",   # Aland
    "GBA": "GBA",  # Alderney
    "FO":  "FO",   # Faroe Islands
    "GBZ": "GBZ",  # Gibraltar
    "GBG": "GBG",  # Guernsey
    "GBM": "GBM",  # Isle of Man
    "GBJ": "GBJ",  # Jersey
    # Disputed territories
    "ABH": "ABH",  # Abkhazia
    "RKS": "XK",   # Kosovo
    "TRNC": "TRNC", # Northern Cyprus
    "RSO": "RSO",  # South Ossetia
    "PMR": "PMR",  # Transnistria
}

# Friendly names for codes that aren't standard ISO alpha-2
TERRITORY_NAMES = {
    "AL": "Albania", "AD": "Andorra", "AT": "Austria", "BY": "Belarus",
    "BE": "Belgium", "BA": "Bosnia and Herzegovina", "BG": "Bulgaria",
    "HR": "Croatia", "CY": "Cyprus", "CZ": "Czech Republic", "DK": "Denmark",
    "EE": "Estonia", "FI": "Finland", "FR": "France", "DE": "Germany",
    "GR": "Greece", "HU": "Hungary", "IS": "Iceland", "IE": "Ireland",
    "IT": "Italy", "LV": "Latvia", "LI": "Liechtenstein", "LT": "Lithuania",
    "LU": "Luxembourg", "MT": "Malta", "MD": "Moldova", "MC": "Monaco",
    "ME": "Montenegro", "NL": "Netherlands", "MK": "North Macedonia",
    "NO": "Norway", "PL": "Poland", "PT": "Portugal", "RO": "Romania",
    "SM": "San Marino", "RS": "Serbia", "SK": "Slovakia", "SI": "Slovenia",
    "ES": "Spain", "SE": "Sweden", "CH": "Switzerland", "UA": "Ukraine",
    "GB": "United Kingdom", "VA": "Vatican City",
    "AM": "Armenia", "AZ": "Azerbaijan", "GE": "Georgia", "RU": "Russia",
    "TR": "Turkey",
    "AX": "Aland Islands", "GBA": "Alderney", "FO": "Faroe Islands",
    "GBZ": "Gibraltar", "GBG": "Guernsey", "GBM": "Isle of Man",
    "GBJ": "Jersey",
    "ABH": "Abkhazia", "XK": "Kosovo", "TRNC": "Northern Cyprus",
    "RSO": "South Ossetia", "PMR": "Transnistria",
}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def strip_html(html_str: str) -> str:
    """Remove HTML tags and unescape entities."""
    return unescape(re.sub(r"<[^>]+>", "", html_str)).strip()


def mime_to_ext(mime: str) -> str:
    return {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/svg+xml": ".svg",
        "image/gif": ".gif",
        "image/tiff": ".tiff",
        "image/webp": ".webp",
    }.get(mime, ".jpg")


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _api_get(session: requests.Session, api_url: str, params: dict) -> dict:
    """Rate-limited GET to a MediaWiki API endpoint."""
    time.sleep(REQUEST_DELAY)
    params["format"] = "json"
    resp = session.get(api_url, params=params)
    if resp.status_code == 429:
        wait = int(resp.headers.get("Retry-After", 60))
        print(f"  Rate-limited -- waiting {wait}s")
        time.sleep(wait)
        resp = session.get(api_url, params=params)
    resp.raise_for_status()
    return resp.json()


def fetch_wikitext(session: requests.Session) -> str:
    """Fetch the raw wikitext of the Wikipedia article."""
    data = _api_get(session, WIKIPEDIA_API_URL, {
        "action": "parse",
        "page": ARTICLE_TITLE,
        "prop": "wikitext",
    })
    return data["parse"]["wikitext"]["*"]


def get_image_info(session: requests.Session, file_title: str) -> dict | None:
    """Get download URL, license, dimensions, and attribution for a Commons file."""
    data = _api_get(session, COMMONS_API_URL, {
        "action": "query",
        "titles": file_title,
        "prop": "imageinfo",
        "iiprop": "url|extmetadata|size|mime",
        "iiextmetadatafilter": "LicenseShortName|Artist|ImageDescription|Credit|AttributionRequired|Restrictions",
    })
    pages = data.get("query", {}).get("pages", {})
    for page_id, page_data in pages.items():
        if page_id == "-1":
            return None
        info = page_data.get("imageinfo", [{}])[0]
        ext = info.get("extmetadata", {})
        return {
            "url": info.get("url"),
            "descriptionurl": info.get("descriptionurl"),
            "width": info.get("width", 0),
            "height": info.get("height", 0),
            "mime": info.get("mime", ""),
            "license": ext.get("LicenseShortName", {}).get("value", "Unknown"),
            "artist": ext.get("Artist", {}).get("value", "Unknown"),
            "description": ext.get("ImageDescription", {}).get("value", ""),
            "credit": ext.get("Credit", {}).get("value", ""),
            "attribution_required": ext.get("AttributionRequired", {}).get("value", ""),
            "restrictions": ext.get("Restrictions", {}).get("value", ""),
        }
    return None


def download_image(session: requests.Session, url: str, dest: str) -> None:
    """Download a file from URL to local path."""
    time.sleep(REQUEST_DELAY)
    resp = session.get(url, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(8192):
            f.write(chunk)


# ---------------------------------------------------------------------------
# Wikitext parsing
# ---------------------------------------------------------------------------

def parse_plate_tables(wikitext: str) -> list[dict]:
    """Parse the wikitables to extract country code and Example-column image.

    Returns a list of dicts: {"wiki_code": str, "image_file": str, "section": str}
    """
    entries = []

    # Locate the three/four table sections
    sections = [
        ("countries", "=== Countries ==="),
        ("transcontinental", "=== Transcontinental countries ==="),
        ("dependent", "=== Dependent territories ==="),
        ("disputed", "=== Disputed territories ==="),
    ]

    for section_name, header in sections:
        start = wikitext.find(header)
        if start == -1:
            print(f"  Warning: section '{header}' not found in wikitext")
            continue

        # Find the table end (|}), starting after the header
        table_start = wikitext.find("{|", start)
        table_end = wikitext.find("|}", table_start)
        if table_start == -1 or table_end == -1:
            continue

        table_text = wikitext[table_start:table_end]

        # Split into rows on "|-"
        rows = re.split(r"\n\|-", table_text)

        for row in rows[1:]:  # skip header row
            entry = _parse_table_row(row, section_name)
            if entry:
                entries.append(entry)

    return entries


def _parse_table_row(row_text: str, section: str) -> dict | None:
    """Extract the wiki code and the first Example-column image from a table row."""
    # Split row into cells on "||"
    # The columns are: Country | Code | Strip | Example | Motorcycle | (Moped)
    # But wikitext formatting varies: cells might use "||" or start with "|"

    # Extract the registration code from the Code column
    # Pattern: [[Vehicle registration plates of ...|CODE]]
    code_match = re.search(
        r'\[\[Vehicle registration plates of [^|]+\|([A-Z]+)\]\]',
        row_text
    )
    if not code_match:
        return None

    wiki_code = code_match.group(1)

    # Find all [[File:...]] or [[Image:...]] references in the row
    file_matches = re.findall(
        r'\[\[(?:File|Image):([^|\]]+)',
        row_text
    )

    if not file_matches:
        return None

    # The columns are: Country | Code | Strip | Example | Motorcycle | Moped
    # Strip images are eurobands (small ~100px), Example images are plates (~200px)
    # We need to identify the Example column image.
    #
    # Strategy: skip images that look like eurobands/strips (contain "euroband",
    # "band", "section", "EU-section", "Non-EU-section", "Identifier") and
    # pick the first remaining image.
    strip_patterns = [
        "euroband", "eurobamd",  # typo on wiki for Estonia
        "-band.", "band.png", "band.svg",
        "section-with", "section_with", "EU-section",
        "Non-EU-section", "Identifier", "Number Plate Band",
        "Blank Rear Identifier",
    ]

    example_image = None
    for filename in file_matches:
        filename_clean = filename.strip()
        is_strip = any(p.lower() in filename_clean.lower() for p in strip_patterns)
        if not is_strip:
            example_image = filename_clean
            break

    if not example_image:
        return None

    # Normalise the filename (underscores for spaces is how Commons works)
    iso = WIKI_CODE_TO_ISO.get(wiki_code, wiki_code)

    return {
        "wiki_code": wiki_code,
        "iso": iso,
        "name": TERRITORY_NAMES.get(iso, wiki_code),
        "image_file": f"File:{example_image}",
        "section": section,
    }


# ---------------------------------------------------------------------------
# Per-entry processing
# ---------------------------------------------------------------------------

def process_entry(entry: dict, output_dir: Path, session: requests.Session,
                  dry_run: bool) -> dict:
    """Resolve image info and download for one country/territory."""
    iso = entry["iso"]
    name = entry["name"]
    image_file = entry["image_file"]

    print(f"\n[{iso}] {name}")
    print(f"  Wiki image: {image_file}")

    info = get_image_info(session, image_file)
    if info is None:
        print(f"  Could not resolve image info")
        return {"status": "failed", "reason": f"Image not found on Commons: {image_file}"}

    if dry_run:
        print(f"  {info.get('width', '?')}x{info.get('height', '?')} "
              f"[{info.get('mime', '?')}] license={info.get('license', '?')}")
        return {
            "status": "success_dry",
            "image_file": image_file,
            "mime": info.get("mime", ""),
            "width": info.get("width", 0),
            "height": info.get("height", 0),
            "license": info.get("license", "Unknown"),
        }

    ext = mime_to_ext(info.get("mime", "image/jpeg"))
    filename = f"{iso}_wiki{ext}"
    dest = output_dir / "plates" / "europe" / filename

    print(f"  Downloading: {info.get('url', '')[:80]}...")
    download_image(session, info["url"], str(dest))
    print(f"  Saved: {filename}")

    return {
        "status": "success",
        "country_name": name,
        "iso": iso,
        "section": entry["section"],
        "file_name": filename,
        "local_path": str(dest.relative_to(output_dir.parent)),
        "source_file": image_file,
        "source_url": info.get("descriptionurl", ""),
        "download_url": info.get("url", ""),
        "license": info.get("license", "Unknown"),
        "artist": strip_html(info.get("artist", "Unknown")),
        "artist_html": info.get("artist", ""),
        "attribution_required": info.get("attribution_required", "") == "true",
        "credit_line": strip_html(info.get("credit", "")),
        "description": strip_html(info.get("description", "")),
        "image_width": info.get("width", 0),
        "image_height": info.get("height", 0),
        "mime_type": info.get("mime", ""),
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Download European license plate images from Wikipedia article")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and resolve without downloading")
    parser.add_argument("--country", type=str,
                        help="Process only one country (ISO code, e.g. DE)")
    parser.add_argument("--output-dir", type=str, default="dataset",
                        help="Output directory (default: dataset)")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    (output_dir / "plates" / "europe").mkdir(parents=True, exist_ok=True)
    (output_dir / "metadata").mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # 1. Fetch and parse the Wikipedia article
    print("Fetching Wikipedia article wikitext...")
    wikitext = fetch_wikitext(session)
    entries = parse_plate_tables(wikitext)
    print(f"Parsed {len(entries)} entries from wikitables")

    # 2. Filter if --country specified
    if args.country:
        code = args.country.upper()
        entries = [e for e in entries if e["iso"] == code]
        if not entries:
            print(f"Unknown country code: {code}")
            print("Available codes:")
            all_entries = parse_plate_tables(wikitext)
            for e in all_entries:
                print(f"  {e['iso']:5s}  {e['name']}")
            return

    # 3. Process each entry
    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "Wikipedia - European vehicle registration plate",
        "source_url": f"https://en.wikipedia.org/wiki/{ARTICLE_TITLE}",
        "entries": {},
    }
    log_results = {}

    for entry in entries:
        iso = entry["iso"]
        result = process_entry(entry, output_dir, session, args.dry_run)

        if result.get("status") in ("success", "success_dry"):
            metadata["entries"][iso] = result
            log_results[iso] = {
                "status": result["status"],
                "image_file": entry["image_file"],
            }
        else:
            log_results[iso] = result

        # Write metadata incrementally
        if not args.dry_run:
            with open(output_dir / "metadata" / "wiki_plates.json", "w") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

    # 4. Write final log
    log = {
        "run_started": metadata["generated_at"],
        "run_completed": datetime.now(timezone.utc).isoformat(),
        "dry_run": args.dry_run,
        "total_entries": len(entries),
        "successful": sum(1 for r in log_results.values()
                          if "success" in r.get("status", "")),
        "failed": sum(1 for r in log_results.values()
                      if r.get("status") == "failed"),
        "results": log_results,
    }
    with open(output_dir / "metadata" / "wiki_download_log.json", "w") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)

    print(f"\nDone: {log['successful']}/{log['total_entries']} entries "
          f"({'dry run' if args.dry_run else 'downloaded'})")
    if log["failed"] > 0:
        print("Failed entries:")
        for iso, r in log_results.items():
            if r.get("status") == "failed":
                print(f"  {iso}: {r.get('reason', 'unknown')}")


if __name__ == "__main__":
    main()
