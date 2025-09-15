import os
import zipfile
import logging
from datetime import datetime
import xml.etree.ElementTree as ET
import pandas as pd

# ---------------------------
# Logging Configuration
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------
# File Extraction
# ---------------------------
def extract_all_zips(zip_dir, extract_to=None):
    """
    Extract all ZIP files in a directory.
    Returns the list of extracted XML file paths.
    """
    if extract_to is None:
        extract_to = zip_dir

    extracted_files = []
    for file in os.listdir(zip_dir):
        if file.lower().endswith(".zip"):
            zip_path = os.path.join(zip_dir, file)
            logger.info(f"Extracting {zip_path} to {extract_to}")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_to)

                # Track only XML files extracted
                for name in zip_ref.namelist():
                    if name.lower().endswith(".xml"):
                        extracted_files.append(os.path.join(extract_to, name))

    return extracted_files


# ---------------------------
# XML Parsing
# ---------------------------
def parse_abr_file(xml_file):
    """
    Parse a single XML file containing ABR data.
    Returns a list of records (dicts).
    """
    records = []
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for abr in root.findall(".//ABR"):
            abn_elem = abr.find("ABN")

            record = {
                "ABN": abn_elem.text if abn_elem is not None else "",
                "ABN_Status": abn_elem.get("status") if abn_elem is not None else "",
                "ABN_Status_From": abn_elem.get("ABNStatusFromDate") if abn_elem is not None else "",
                "Entity_Type_Code": abr.findtext("EntityType/EntityTypeInd", ""),
                "Entity_Type": abr.findtext("EntityType/EntityTypeText", ""),
                "Entity_Name": abr.findtext("MainEntity/NonIndividualName/NonIndividualNameText", ""),
                "Trading_Names": "; ".join([
                    n.text for n in abr.findall("OtherEntity/NonIndividualName/NonIndividualNameText")
                    if n is not None and n.text
                ]),
                "ASIC_Number": abr.findtext("ASICNumber", ""),
                "GST_Status": abr.find("GST").get("status") if abr.find("GST") is not None else "",
                "GST_From": abr.find("GST").get("GSTStatusFromDate") if abr.find("GST") is not None else "",
                "State": abr.findtext("MainEntity/BusinessAddress/AddressDetails/State", ""),
                "Postcode": abr.findtext("MainEntity/BusinessAddress/AddressDetails/Postcode", ""),
                "Record_Last_Updated": abr.get("recordLastUpdatedDate", "")
            }
            records.append(record)

    except ET.ParseError as e:
        logger.error(f"Failed to parse {xml_file}: {e}")

    return records


# ---------------------------
# Processing Pipeline
# ---------------------------
def process_all_xml(xml_files, output_csv):
    """
    Convert multiple XML files into a single CSV file.
    """
    all_records = []

    for xml_file in xml_files:
        records = parse_abr_file(xml_file)
        if not records:
            logger.warning(f"No records found in {xml_file}")
            continue

        all_records.extend(records)
        logger.info(f"Parsed {len(records)} records from {xml_file}")

    if not all_records:
        logger.warning("No records found in any XML file.")
        return

    df = pd.DataFrame(all_records)
    df.to_csv(output_csv, index=False)
    logger.info(f"Saved {len(df)} total records → {output_csv}")



# ---------------------------
# Main Entry
# ---------------------------
if __name__ == "__main__":
    base_dir = "/path/of/zips"
    output_dir = os.path.join(base_dir, "csv_output")

    # Step 1: Extract all zips & collect XML files
    xml_files = extract_all_zips(base_dir)

    # Step 2: Process each XML into individual CSVs
    process_all_xml(xml_files, output_dir)
