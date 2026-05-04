#!/usr/bin/env python3
"""
Seagull Maritime — Sanctions Database Refresh Script v3
Fetches latest sanctions data from OFAC SDN, UK FCDO, EU FSF, and
OpenSanctions vessel designations (re-tagged under the authoritative
regime that listed each vessel).

Outputs sanctions_db.json for use with the Seagull Sanctions Screener.

v3 changes (4 May 2026):
    - Added User-Agent header to all fetches (Treasury blocks default UA)
    - OFAC URL refresh: primary endpoint now points at working SDN.xml endpoint
    - Added fetch_opensanctions_vessels(): pulls shadow-fleet and other
      vessel designations from OpenSanctions, tags each by underlying
      authority (OFAC / UK / EU) so they surface under existing source
      badges. Closes the gap where EU 833/2014 Annex XLII vessels and UK
      Russia Regulations vessel designations are not in the consolidated
      financial-sanctions feeds.
    - Per-source success/failure tracked separately

v2 changes (16 April 2026):
    - Fixed UK CSV parser: skip metadata row before column headers
    - Added merge-not-replace: failed sources preserve existing entries
    - Added safety check: refuses to write DB with < 500 entries
    - Added --no-merge flag for clean rebuilds

Usage:
    cd seagull-portals/campaign/data
    python refresh_sanctions.py

Sources:
    - OFAC SDN (US Treasury)
    - UK Sanctions List (FCDO/OFSI)
    - EU FSF (Financial Sanctions)
    - OpenSanctions vessel designations (vessels tagged by authority)

Requirements:
    pip install requests lxml
"""

import json
import hashlib
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

try:
    from lxml import etree
except ImportError:
    print("ERROR: 'lxml' package required. Install with: pip install lxml")
    sys.exit(1)


# Shared HTTP session with built-in retry on connection errors (DNS misses,
# transient TCP failures) and 5xx server errors. Backoff doubles each attempt
# (~0s, 2s, 4s, 8s, 16s) so flaky home/office DNS gets a second chance to
# resolve before we give up on a URL.
def make_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,         # retry on DNS / connection failures (this is the key one)
        read=3,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


SESSION = make_session()


# === SOURCE URLs ===

# OFAC: primary is the live API endpoint that 302-redirects to a presigned S3
# URL. With User-Agent + allow_redirects=True, requests follows the chain.
OFAC_SDN_URLS = [
    "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.xml",
    "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML",
    "https://www.treasury.gov/ofac/downloads/sdn.xml",
    "https://data.opensanctions.org/datasets/latest/us_ofac_sdn/source.xml",
]
UK_SANCTIONS_URLS = [
    # Current canonical (FCDO unified UK Sanctions List, replaced ConList on 28 Jan 2026)
    "https://sanctionslist.fcdo.gov.uk/docs/UK-Sanctions-List.csv",
    # Legacy OFSI ConList endpoints — withdrawn 28 Jan 2026, only useful for historical fallback
    "https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv",
    "https://assets.publishing.service.gov.uk/media/65a27d3ee8f5ec000d1f8b51/ConList.csv",
]
EU_FSF_URLS = [
    "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content",
    "https://webgate.ec.europa.eu/europeaid/fsd/fsf/public/files/xmlFullSanctionsList/content",
    "https://data.opensanctions.org/datasets/latest/eu_fsf/source.xml",
]

# OpenSanctions targets.simple.csv aggregated `sanctions` collection — used
# to top up vessel designations not covered by the consolidated feeds.
OPENSANCTIONS_VESSELS_URL = (
    "https://data.opensanctions.org/datasets/latest/sanctions/targets.simple.csv"
)

# Default request headers — Treasury, OFSI and OpenSanctions endpoints
# tend to filter unusual / Python-default UAs. Mimic a current desktop
# browser so requests pass through cleanly. (Custom branded UAs got
# blocked by HMT/OFSI in v3.0 testing.)
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/xml,application/xml,application/json,text/csv,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}


def make_id(source_prefix, name, index):
    raw = f"{source_prefix}_{name}_{index}"
    return f"{source_prefix}_{hashlib.md5(raw.encode()).hexdigest()[:10]}"


def classify_type(raw_type):
    raw = raw_type.lower().strip() if raw_type else ""
    if raw in ("individual", "person"):
        return "individual"
    elif raw in ("vessel", "aircraft"):
        return "vessel"
    else:
        return "company"


def fetch_with_fallback(urls, description):
    for url in urls:
        try:
            print(f"  Trying: {url[:80]}...")
            # Use the shared SESSION with built-in retry-on-DNS-failure logic
            resp = SESSION.get(
                url,
                timeout=180,
                allow_redirects=True,
                headers=HTTP_HEADERS,
            )
            resp.raise_for_status()
            sz_kb = len(resp.content) / 1024
            if sz_kb < 1:
                # Genuinely empty / placeholder response — treat as failure
                raise Exception(f"empty response ({len(resp.content)} bytes)")
            print(f"  OK ({sz_kb / 1024:.1f} MB)")
            return resp
        except Exception as e:
            print(f"  Failed: {e}")
    raise Exception(f"All URLs failed for {description}")


# === OFAC SDN PARSER ===

def fetch_ofac():
    print("Fetching OFAC SDN list...")
    resp = fetch_with_fallback(OFAC_SDN_URLS, "OFAC SDN")
    root = etree.fromstring(resp.content)
    ns = {"sdn": "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML"}
    if root.find(".//sdn:sdnEntry", ns) is None:
        ns_uri = root.nsmap.get(None, "")
        ns = {"sdn": ns_uri} if ns_uri else {}
    entries = []
    sdn_entries = root.findall(".//sdn:sdnEntry", ns) if ns else root.findall(".//sdnEntry")
    if not sdn_entries:
        for elem in root.iter():
            if elem.tag.endswith("sdnEntry"):
                sdn_entries.append(elem)
                if not ns:
                    tag = elem.tag
                    ns_uri = tag.split("}")[0].lstrip("{") if "}" in tag else ""
                    ns = {"sdn": ns_uri} if ns_uri else {}
                    sdn_entries = root.findall(f".//{{{ns_uri}}}sdnEntry") if ns_uri else []
                    break
    print(f"  Found {len(sdn_entries)} OFAC entries to process...")
    for i, sdn in enumerate(sdn_entries):
        def find_text(parent, tag, default=""):
            if ns:
                el = parent.find(f"sdn:{tag}", ns)
            else:
                el = None
                for child in parent:
                    if child.tag.endswith(tag):
                        el = child
                        break
            return el.text.strip() if el is not None and el.text else default
        def find_all(parent, tag):
            if ns:
                return parent.findall(f"sdn:{tag}", ns)
            return [child for child in parent if child.tag.endswith(tag)]
        name_parts = []
        last_name = find_text(sdn, "lastName")
        first_name = find_text(sdn, "firstName")
        if last_name:
            name_parts.append(last_name)
        if first_name:
            name_parts.append(first_name)
        name = ", ".join(name_parts) if name_parts else find_text(sdn, "sdnName", "UNKNOWN")
        sdn_type = find_text(sdn, "sdnType", "Entity")
        aliases = []
        aka_list = find_all(sdn, "akaList")
        if aka_list:
            for aka_entry in find_all(aka_list[0], "aka"):
                aka_last = find_text(aka_entry, "lastName")
                aka_first = find_text(aka_entry, "firstName")
                if aka_last or aka_first:
                    aliases.append(", ".join(filter(None, [aka_last, aka_first])))
        program_list = find_all(sdn, "programList")
        programs = []
        if program_list:
            for prog in find_all(program_list[0], "program"):
                if prog.text:
                    programs.append(prog.text.strip())
        program = "; ".join(programs)
        nationality = find_text(sdn, "nationality")
        dob = ""
        id_number = ""
        remarks = find_text(sdn, "remarks")
        title = find_text(sdn, "title")
        id_list = find_all(sdn, "idList")
        if id_list:
            for id_entry in find_all(id_list[0], "id"):
                id_type = find_text(id_entry, "idType")
                id_num = find_text(id_entry, "idNumber")
                if id_type and "passport" in id_type.lower() and id_num:
                    id_number = id_num
                    break
                elif id_num and not id_number:
                    id_number = id_num
        dob_list = find_all(sdn, "dateOfBirthList")
        if dob_list:
            for dob_entry in find_all(dob_list[0], "dateOfBirthItem"):
                dob_val = find_text(dob_entry, "dateOfBirth")
                if dob_val:
                    dob = dob_val
                    break
        nat_list = find_all(sdn, "nationalityList")
        if nat_list:
            for nat_entry in find_all(nat_list[0], "nationality"):
                nat_country = find_text(nat_entry, "country")
                if nat_country:
                    nationality = nat_country
                    break
        entry_type = "vessel" if sdn_type.lower() == "vessel" else classify_type(sdn_type)
        vessel_info = {}
        if entry_type == "vessel":
            vessel_details = find_all(sdn, "vesselInfo")
            if vessel_details:
                vi = vessel_details[0]
                vessel_info = {
                    "flag": find_text(vi, "vesselFlag"),
                    "vessel_type": find_text(vi, "vesselType"),
                    "grt": find_text(vi, "tonnage"),
                    "call_sign": find_text(vi, "callSign"),
                    "owner": find_text(vi, "vesselOwner"),
                }
        entry = {
            "id": make_id("OFAC", name, i), "source": "OFAC SDN", "type": entry_type,
            "name": name, "aliases": aliases, "nationality": nationality,
            "program": program, "dob": dob, "id_number": id_number,
            "remarks": remarks, "title": title,
        }
        if vessel_info:
            entry["vessel_info"] = vessel_info
        entries.append(entry)
    print(f"  Processed {len(entries)} OFAC entries")
    return entries


# === UK SANCTIONS LIST PARSER ===

def fetch_uk():
    """Parse UK sanctions CSV. Handles both:
       - New FCDO unified UK Sanctions List (sanctionslist.fcdo.gov.uk, post-28-Jan-2026)
       - Legacy OFSI ConList (deprecated 28 Jan 2026, kept as fallback)
    Tolerant of column name variation between the two schemas."""
    print("Fetching UK Sanctions List...")
    import csv
    import io
    resp = fetch_with_fallback(UK_SANCTIONS_URLS, "UK Sanctions List")
    # Some UK CSVs have a metadata row before the real headers.
    lines = resp.text.splitlines()
    if not lines:
        return []
    # Detect format from header row — check for known column markers.
    # Strip any leading metadata rows until we find a row that looks like headers.
    header_row_idx = 0
    for idx, line in enumerate(lines[:5]):
        if any(marker in line for marker in ["Name 6", "Primary Name", "DesignationName", "Name_6", "Name1"]):
            header_row_idx = idx
            break
    if header_row_idx > 0:
        print(f"  Skipping {header_row_idx} metadata row(s) before headers")
        lines = lines[header_row_idx:]

    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    cols = reader.fieldnames or []
    print(f"  Detected {len(cols)} columns: {cols[:8]}{'...' if len(cols) > 8 else ''}")

    # Field-name candidates — try in order, first non-empty wins.
    NAME_FIELDS = [
        "Name 6", "Name_6", "Primary Name", "Name", "DesignationName",
        "PrimaryName", "FullName", "name",
    ]
    NAME_PART_FIELDS = ["Name 1", "Name 2", "Name 3", "Name 4", "Name 5", "Name 6"]
    TYPE_FIELDS = ["Group Type", "GroupType", "Type", "EntityType", "Subject Type"]
    REGIME_FIELDS = ["Regime", "regime", "SanctionsRegime", "Programme", "Program"]
    COUNTRY_FIELDS = ["Country", "Nationality", "CountryOfBirth"]
    DOB_FIELDS = ["DOB", "Date of Birth", "DateOfBirth", "BirthDate"]
    ID_FIELDS = [
        "Passport Number", "PassportNumber",
        "National Identification Number", "NationalID", "IDNumber",
        "Identification Number",
    ]
    REMARKS_FIELDS = ["Other Information", "OtherInformation", "Notes", "Remarks", "Reasons", "Statement of Reasons"]
    POSITION_FIELDS = ["Position", "Title", "Role"]

    def first_nonempty(row, candidates):
        for k in candidates:
            v = row.get(k)
            if v and str(v).strip():
                return str(v).strip()
        return ""

    entries = []
    seen = {}
    skipped_no_name = 0
    for i, row in enumerate(reader):
        name = first_nonempty(row, NAME_FIELDS)
        if not name:
            # Fallback: assemble from name-parts (legacy ConList pattern)
            parts = [v for v in (row.get(k, "") for k in NAME_PART_FIELDS) if v]
            name = " ".join(parts).strip()
        if not name:
            skipped_no_name += 1
            continue

        group_type = first_nonempty(row, TYPE_FIELDS).lower()
        if "individual" in group_type or "person" in group_type:
            entry_type = "individual"
        elif "ship" in group_type or "vessel" in group_type:
            entry_type = "vessel"
        else:
            entry_type = "company"

        aliases = []
        for j in range(1, 30):
            for tmpl in (f"Alias {j}", f"Alias_{j}", f"Aka {j}", f"AKA {j}"):
                a = row.get(tmpl, "")
                if a and str(a).strip():
                    aliases.append(str(a).strip())
                    break

        dedup_key = f"{name}_{entry_type}"
        if dedup_key in seen:
            existing = seen[dedup_key]
            for a in aliases:
                if a not in existing["aliases"]:
                    existing["aliases"].append(a)
            continue

        nationality = first_nonempty(row, COUNTRY_FIELDS)
        regime = first_nonempty(row, REGIME_FIELDS)
        dob = first_nonempty(row, DOB_FIELDS)
        id_number = first_nonempty(row, ID_FIELDS)
        remarks = first_nonempty(row, REMARKS_FIELDS)[:500]
        title = first_nonempty(row, POSITION_FIELDS)

        entry = {
            "id": make_id("UK", name, i), "source": "UK Sanctions List", "type": entry_type,
            "name": name, "aliases": aliases, "nationality": nationality,
            "program": regime, "dob": dob, "id_number": id_number,
            "remarks": remarks, "title": title,
        }
        entries.append(entry)
        seen[dedup_key] = entry
    print(f"  Processed {len(entries)} UK entries (skipped {skipped_no_name} rows with no name)")
    return entries


# === EU FSF PARSER ===

def fetch_eu():
    print("Fetching EU Financial Sanctions list...")
    resp = fetch_with_fallback(EU_FSF_URLS, "EU FSF")
    root = etree.fromstring(resp.content)
    entries = []
    for i, entity in enumerate(root.iter()):
        if not entity.tag.endswith("sanctionEntity"):
            continue
        programme = ""
        for reg in entity.iter():
            if reg.tag.endswith("programme"):
                programme = reg.text.strip() if reg.text else ""
                break
        name = ""
        aliases = []
        entity_type = "company"
        for name_alias in entity.iter():
            if name_alias.tag.endswith("nameAlias"):
                whole_name = name_alias.get("wholeName", "")
                if not name and whole_name:
                    name = whole_name
                elif whole_name and whole_name != name:
                    aliases.append(whole_name)
        if not name:
            continue
        for sub_type in entity.iter():
            if sub_type.tag.endswith("subjectType"):
                code = sub_type.get("code", "") or (sub_type.text or "")
                if "person" in code.lower():
                    entity_type = "individual"
                elif "enterprise" in code.lower() or "entity" in code.lower():
                    entity_type = "company"
                break
        dob = ""
        for bd in entity.iter():
            if bd.tag.endswith("birthdate"):
                dob = bd.get("birthdate", "") or ""
                if not dob:
                    y = bd.get("year", "")
                    m = bd.get("month", "")
                    d = bd.get("day", "")
                    if y:
                        dob = f"{y}-{m or '??'}-{d or '??'}"
                break
        nationality = ""
        for cit in entity.iter():
            if cit.tag.endswith("citizenship"):
                nationality = cit.get("countryDescription", "") or ""
                break
        id_number = ""
        for ident in entity.iter():
            if ident.tag.endswith("identification"):
                id_number = ident.get("number", "") or ""
                break
        remarks = ""
        for rem in entity.iter():
            if rem.tag.endswith("remark"):
                if rem.text:
                    remarks = rem.text.strip()[:500]
                break
        entry = {
            "id": make_id("EU", name, i), "source": "EU FSF", "type": entity_type,
            "name": name, "aliases": aliases[:20], "nationality": nationality,
            "program": programme, "dob": dob, "id_number": id_number,
            "remarks": remarks, "title": "",
        }
        entries.append(entry)
    print(f"  Processed {len(entries)} EU entries")
    return entries


# === OPENSANCTIONS VESSEL DESIGNATIONS PARSER ===

def fetch_opensanctions_vessels():
    """Fetch vessel designations from the OpenSanctions `sanctions` aggregate.
    Tags each vessel by the underlying authority that listed it (OFAC SDN /
    UK Sanctions List / EU FSF) so vessels appear under existing source
    badges in the screener UI. Closes the gap where the primary consolidated
    feeds (OFAC SDN, UK Consolidated, EU FSF) do not include shadow-fleet
    vessel designations under EU 833/2014 Annex XLII or UK Russia
    Regulations vessel listings."""

    print("Fetching OpenSanctions vessel designations...")

    # Map OpenSanctions dataset codes -> our existing source labels.
    # Vessels in datasets not in this map are skipped (don't introduce new badges).
    DATASET_TO_SOURCE = {
        # OFAC (US Treasury)
        "us_ofac_sdn": "OFAC SDN",
        "us_ofac_cons": "OFAC SDN",
        "ofac_sdn": "OFAC SDN",
        # UK (HMT/OFSI/FCDO)
        "gb_hmt_sanctions": "UK Sanctions List",
        "gb_fcdo_sanctions": "UK Sanctions List",
        "gb_uksl": "UK Sanctions List",
        # EU
        "eu_fsf": "EU FSF",
        "eu_sanctions_map": "EU FSF",
        "eu_meas": "EU FSF",
        "eu_council_decisions": "EU FSF",
    }

    URL = "https://data.opensanctions.org/datasets/latest/sanctions/entities.ftm.json"
    try:
        # Use shared SESSION (with retry-on-DNS-failure) for the OpenSanctions pull too
        resp = SESSION.get(URL, timeout=300, stream=True, headers=HTTP_HEADERS)
        resp.raise_for_status()
    except Exception as e:
        print(f"  WARNING: OpenSanctions fetch failed: {e}")
        return []

    entries = []
    seen_ids = set()
    line_count = 0
    vessel_count = 0
    skipped_unknown_dataset = 0

    for line in resp.iter_lines(decode_unicode=True):
        if not line:
            continue
        line_count += 1
        try:
            ent = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ent.get("schema") != "Vessel":
            continue
        vessel_count += 1

        props = ent.get("properties", {}) or {}
        names = props.get("name", []) or []
        if not names:
            continue
        primary_name = names[0]

        aliases_raw = (
            names[1:]
            + (props.get("alias", []) or [])
            + (props.get("previousName", []) or [])
            + (props.get("weakAlias", []) or [])
        )
        seen_alias = set()
        aliases = []
        for a in aliases_raw:
            if a and a not in seen_alias and a != primary_name:
                seen_alias.add(a)
                aliases.append(a)
            if len(aliases) >= 20:
                break

        imo_list = props.get("imoNumber", []) or []
        primary_imo = imo_list[0] if imo_list else ""
        flag_list = props.get("flag", []) or []
        type_list = props.get("type", []) or []
        call_sign = props.get("callSign", []) or []
        mmsi_list = props.get("mmsi", []) or []
        tonnage = props.get("tonnage", []) or []
        program = props.get("program", []) or []
        notes = props.get("notes", []) or []
        topics = props.get("topics", []) or []

        ent_datasets = ent.get("datasets", []) or []
        sources = set()
        for ds in ent_datasets:
            tag = DATASET_TO_SOURCE.get(ds)
            if tag:
                sources.add(tag)
        if not sources:
            skipped_unknown_dataset += 1
            continue

        remarks_parts = []
        if primary_imo:
            remarks_parts.append(f"(IMO Number):{primary_imo}")
        if flag_list:
            remarks_parts.append(f"(Flag):{flag_list[0]}")
        if topics:
            remarks_parts.append(f"(Topics):{','.join(topics)}")
        if ent_datasets:
            remarks_parts.append(f"(Source datasets):{','.join(ent_datasets)}")
        if notes:
            note_text = notes[0] if isinstance(notes, list) else str(notes)
            if note_text:
                remarks_parts.append(note_text[:200])
        remarks = ". ".join(remarks_parts)[:500]

        for src in sources:
            entry_id = make_id(
                src.replace(" ", "_").upper(),
                primary_name + "_OS_" + (primary_imo or "NOIMO"),
                line_count,
            )
            if entry_id in seen_ids:
                continue
            seen_ids.add(entry_id)
            entries.append({
                "id": entry_id,
                "source": src,
                "type": "vessel",
                "name": primary_name,
                "aliases": aliases,
                "nationality": flag_list[0] if flag_list else "",
                "program": "; ".join(program) if program else "Sanctioned vessel",
                "dob": "",
                "id_number": primary_imo,
                "remarks": remarks,
                "title": "",
                "vessel_info": {
                    "flag": flag_list[0] if flag_list else "",
                    "vessel_type": type_list[0] if type_list else "",
                    "grt": tonnage[0] if tonnage else "",
                    "call_sign": call_sign[0] if call_sign else "",
                    "imo": primary_imo,
                    "mmsi": mmsi_list[0] if mmsi_list else "",
                    "owner": "",
                },
            })

    print(f"  Scanned {line_count} entities, {vessel_count} vessels found")
    print(f"  Created {len(entries)} vessel entries (tagged under existing source badges)")
    if skipped_unknown_dataset:
        print(f"  Skipped {skipped_unknown_dataset} vessels in datasets not mapped to OFAC/UK/EU")
    return entries


# === MERGE + SAFETY ===

def load_existing_db(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        entries = data.get("entries", [])
        print(f"  Loaded existing database: {len(entries)} entries")
        return entries
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  No existing database to merge with: {e}")
        return []


def main():
    ap = argparse.ArgumentParser(description="Refresh Seagull Maritime sanctions database")
    ap.add_argument("--output", "-o", default="sanctions_db.json", help="Output JSON file path")
    ap.add_argument("--ofac-only", action="store_true", help="Only refresh OFAC SDN")
    ap.add_argument("--uk-only", action="store_true", help="Only refresh UK list")
    ap.add_argument("--eu-only", action="store_true", help="Only refresh EU list")
    ap.add_argument("--no-merge", action="store_true", help="Don't merge with existing DB on failure")
    args = ap.parse_args()

    output_path = Path(args.output)
    existing = load_existing_db(output_path) if not args.no_merge else []

    fresh = {}
    failed = []
    fetched = []
    do_all = not (args.ofac_only or args.uk_only or args.eu_only)

    for name, flag, func in [
        ("OFAC SDN", args.ofac_only, fetch_ofac),
        ("UK Sanctions List", args.uk_only, fetch_uk),
        ("EU FSF", args.eu_only, fetch_eu),
    ]:
        if do_all or flag:
            try:
                result = func()
                if result:
                    fresh[name] = result
                    fetched.append(name)
                else:
                    failed.append(name)
            except Exception as e:
                print(f"  WARNING: {name} fetch failed: {e}")
                failed.append(name)

    # Merge: fresh entries + preserved entries from failed sources
    all_entries = []
    preserved = []
    for entries in fresh.values():
        all_entries.extend(entries)
    if failed and existing and not args.no_merge:
        for entry in existing:
            if entry.get("source") in failed:
                all_entries.append(entry)
        preserved = [s for s in failed if any(e.get("source") == s for e in existing)]
        if preserved:
            print(f"\n  MERGE: Preserved existing entries for: {', '.join(preserved)}")

    # Supplementary: OpenSanctions vessel designations (only on full refresh).
    # Tagged by underlying authority (OFAC/UK/EU) so they surface under the
    # existing source badges. Closes the gap where shadow-fleet vessels under
    # EU 833/2014 Annex XLII or UK Russia Regulations vessel designations are
    # not present in the consolidated financial-sanctions feeds.
    os_added = 0
    if do_all:
        try:
            os_vessels = fetch_opensanctions_vessels()
            if os_vessels:
                # Dedup by IMO against existing all_entries — keep the existing
                # entry if same vessel already covered by a primary feed.
                existing_imos_by_source = {}
                for e in all_entries:
                    if e.get("type") == "vessel":
                        imo = (e.get("vessel_info") or {}).get("imo") or e.get("id_number") or ""
                        imo = "".join(c for c in str(imo) if c.isdigit())
                        if imo:
                            key = (e.get("source"), imo)
                            existing_imos_by_source[key] = True
                for v in os_vessels:
                    imo = "".join(c for c in str(v.get("id_number") or "") if c.isdigit())
                    key = (v.get("source"), imo)
                    if imo and key in existing_imos_by_source:
                        continue  # already covered by a primary feed under same source
                    all_entries.append(v)
                    os_added += 1
                print(f"  Added {os_added} new vessel entries from OpenSanctions (skipped {len(os_vessels) - os_added} duplicates)")
        except Exception as e:
            print(f"  WARNING: OpenSanctions supplemental fetch failed (proceeding without): {e}")

    # Safety check
    if len(all_entries) < 500:
        print(f"\n  SAFETY CHECK FAILED: Only {len(all_entries)} entries (min 500).")
        print(f"  Existing database NOT overwritten.")
        print(f"  Failed: {', '.join(failed) if failed else 'none'}")
        sys.exit(1)

    by_source = {}
    by_type = {}
    for e in all_entries:
        by_source[e["source"]] = by_source.get(e["source"], 0) + 1
        by_type[e["type"]] = by_type.get(e["type"], 0) + 1

    output = {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": sorted(set(fetched + preserved)),
            "sources_refreshed": fetched,
            "sources_preserved": preserved,
            "sources_failed": [s for s in failed if s not in preserved],
            "opensanctions_vessels_added": os_added,
            "total_entries": len(all_entries),
            "by_source": by_source,
            "by_type": by_type,
        },
        "entries": all_entries,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False)

    mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\nDone! Generated {output_path}")
    print(f"  Total entries: {len(all_entries)}")
    print(f"  File size: {mb:.1f} MB")
    print(f"  Refreshed: {', '.join(fetched) if fetched else 'none'}")
    if preserved:
        print(f"  Preserved from existing DB: {', '.join(preserved)}")
    if failed:
        print(f"  Failed: {', '.join(failed)}")
    for src, count in by_source.items():
        print(f"    {src}: {count}")


if __name__ == "__main__":
    main()
