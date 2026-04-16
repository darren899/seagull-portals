#!/usr/bin/env python3
"""
Seagull Maritime — Sanctions Database Refresh Script v2
Fetches latest sanctions data from OFAC SDN, UK FCDO, and EU FSF.
Outputs sanctions_db.json for use with the Seagull Sanctions Screener.

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
except ImportError:
    print("ERROR: 'requests' package required. Install with: pip install requests")
    sys.exit(1)

try:
    from lxml import etree
except ImportError:
    print("ERROR: 'lxml' package required. Install with: pip install lxml")
    sys.exit(1)


# === SOURCE URLs ===

OFAC_SDN_URLS = [
    "https://www.treasury.gov/ofac/downloads/sdn.xml",
    "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML",
]
UK_SANCTIONS_URLS = [
    "https://assets.publishing.service.gov.uk/media/65a27d3ee8f5ec000d1f8b51/ConList.csv",
    "https://ofsistorage.blob.core.windows.net/publishlive/2022format/ConList.csv",
    "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/consolidated-list.csv",
]
EU_FSF_URLS = [
    "https://webgate.ec.europa.eu/fsd/fsf/public/files/xmlFullSanctionsList_1_1/content",
    "https://webgate.ec.europa.eu/europeaid/fsd/fsf/public/files/xmlFullSanctionsList/content",
    "https://data.opensanctions.org/datasets/latest/eu_fsf/source.xml",
]


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
            resp = requests.get(url, timeout=120, allow_redirects=True)
            resp.raise_for_status()
            print(f"  OK ({len(resp.content) / 1024 / 1024:.1f} MB)")
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
    print("Fetching UK Sanctions List...")
    import csv
    import io
    resp = fetch_with_fallback(UK_SANCTIONS_URLS, "UK Sanctions List")
    # UK CSV has a metadata row before the real headers.
    lines = resp.text.splitlines()
    if lines and "Name 6" not in lines[0] and len(lines) > 1:
        print("  Skipping metadata row in UK CSV...")
        lines = lines[1:]
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    entries = []
    seen = {}
    for i, row in enumerate(reader):
        name = row.get("Name 6", "") or row.get("name", "") or ""
        if not name:
            parts = []
            for key in ["Name 1", "Name 2", "Name 3", "Name 4", "Name 5", "Name 6"]:
                val = row.get(key, "")
                if val:
                    parts.append(val)
            name = " ".join(parts)
        if not name:
            continue
        group_type = (row.get("Group Type") or "").lower()
        if "individual" in group_type:
            entry_type = "individual"
        elif "ship" in group_type or "vessel" in group_type:
            entry_type = "vessel"
        else:
            entry_type = "company"
        aliases = []
        for j in range(1, 20):
            alias = row.get(f"Alias {j}", "") or ""
            if alias.strip():
                aliases.append(alias.strip())
        dedup_key = f"{name}_{entry_type}"
        if dedup_key in seen:
            existing = seen[dedup_key]
            for a in aliases:
                if a not in existing["aliases"]:
                    existing["aliases"].append(a)
            continue
        nationality = row.get("Country", "") or ""
        regime = row.get("Regime", "") or row.get("regime", "") or ""
        dob = row.get("DOB", "") or row.get("Date of Birth", "") or ""
        id_number = row.get("Passport Number", "") or row.get("National Identification Number", "") or ""
        entry = {
            "id": make_id("UK", name, i), "source": "UK Sanctions List", "type": entry_type,
            "name": name, "aliases": aliases, "nationality": nationality,
            "program": regime, "dob": dob, "id_number": id_number,
            "remarks": (row.get("Other Information", "") or "")[:500],
            "title": row.get("Position", "") or "",
        }
        entries.append(entry)
        seen[dedup_key] = entry
    print(f"  Processed {len(entries)} UK entries")
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
