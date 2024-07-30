import csv
import datetime
import time
import urllib.request
from pathlib import Path
import requests
import typer
from bs4 import BeautifulSoup
import os
import re
import pandas as pd
import PyPDF2

opener = urllib.request.build_opener()
opener.addheaders = [("User-agent", "Mozilla/5.0")]
urllib.request.install_opener(opener)

BASE_URL = "https://www.financial-ombudsman.org.uk/decisions-case-studies/ombudsman-decisions/search"
BASE_DECISIONS_URL = "https://www.financial-ombudsman.org.uk/"
BASE_PARAMETERS = {
    "Sort": "date",
    "Start": 0,
}

INDUSTRY_SECTOR_MAPPING = {
    "banking-credit-mortgages": 1,
    "investment-pensions": 2,
    "insurance": 3,
    "payment-protection-insurance": 4,
    "claims-management-ombudsman-decisions": 5,
    "funeral-plans": 6,
}

app = typer.Typer()

def process_entry(entry):
    anchor = entry.find("a")
    decision_url_part = anchor["href"]
    title = anchor.find("h4").text.strip()
    metadata = anchor.find("div", class_="search-result__info-main").text
    tag = anchor.find("span", class_="search-result__tag").text

    metadata = [m.strip() for m in metadata.strip().split("\n") if m.strip()]
    [date, company, decision, *extras] = metadata
    extras = ",".join(extras)

    decision_id = Path(decision_url_part).stem

    return {
        "decision_id": decision_id,
        "location": decision_url_part,
        "title": title,
        "date": date,
        "company": company,
        "decision": decision,
        "extras": extras,
        "tag": tag.strip(),
    }

@app.command()
def get_metadata_for_years(
    keyword: str = typer.Option(None, help="Keyword to search for"),
    start_year: int = typer.Option(2013, help="The start year for the search"),
    end_year: int = typer.Option(2024, help="The end year for the search"),
    upheld: bool = typer.Option(None, help="Filter by whether the decision was upheld"),
    industry_sector: str = typer.Option(
        "insurance", help="Filter by industry sector, separated by commas. If not provided, all sectors will be included"
    ),
):
    industry_sectors = industry_sector.split(",") if industry_sector else list(INDUSTRY_SECTOR_MAPPING.keys())

    all_metadata_entries = []

    quarters = [
        ('01-01', '03-31'),
        ('04-01', '06-30'),
        ('07-01', '09-30'),
        ('10-01', '12-31')
    ]

    for year in range(start_year, end_year + 1):
        for quarter_start, quarter_end in quarters:
            from_ = datetime.datetime.strptime(f"{year}-{quarter_start}", "%Y-%m-%d")
            to = datetime.datetime.strptime(f"{year}-{quarter_end}", "%Y-%m-%d")
            parameters = BASE_PARAMETERS.copy()

            for selected_industry_sector in industry_sectors:
                parameters[f"IndustrySectorID[{INDUSTRY_SECTOR_MAPPING[selected_industry_sector]}]"] = INDUSTRY_SECTOR_MAPPING[
                    selected_industry_sector
                ]

            if upheld is None:
                parameters["IsUpheld[0]"] = "0"
                parameters["IsUpheld[1]"] = "1"
            elif upheld:
                parameters["IsUpheld[1]"] = "1"
            else:
                parameters["IsUpheld[0]"] = "0"

            parameters["DateFrom"] = from_.strftime("%Y-%m-%d")
            parameters["DateTo"] = to.strftime("%Y-%m-%d")
            if keyword:
                parameters["Keywords"] = keyword

            metadata_entries = []
            for start in range(0, 1_000_000, 10):
                parameters["Start"] = start
                results = requests.get(BASE_URL, params=parameters)

                soup = BeautifulSoup(results.text, "html.parser")

                search_results = soup.find("div", class_="search-results-holder").find("ul", class_="search-results")
                entries = search_results.find_all("li")

                if not entries:
                    typer.echo(f"Finished scraping quarter {from_.strftime('%Y-%m-%d')} to {to.strftime('%Y-%m-%d')} at {start}")
                    break

                typer.echo(f"Scraping {len(entries)} entries from page {start} for quarter {from_.strftime('%Y-%m-%d')} to {to.strftime('%Y-%m-%d')}")

                for entry in entries:
                    processed_entry = process_entry(entry)
                    metadata_entries.append(processed_entry)

            if metadata_entries:
                quarter_name = f"{year}_Q{quarters.index((quarter_start, quarter_end)) + 1}"
                typer.echo(f"Writing {len(metadata_entries)} entries to metadata_{quarter_name}.csv")
                with open(f"metadata_{quarter_name}.csv", "w") as f:
                    writer = csv.DictWriter(f, fieldnames=metadata_entries[0].keys())
                    writer.writeheader()
                    writer.writerows(metadata_entries)

            all_metadata_entries.extend(metadata_entries)

    if all_metadata_entries:
        typer.echo(f"Writing {len(all_metadata_entries)} entries to metadata.csv")
        with open("metadata.csv", "w") as f:
            writer = csv.DictWriter(f, fieldnames=all_metadata_entries[0].keys())
            writer.writeheader()
            writer.writerows(all_metadata_entries)

def format_date(date_str):
    date_parts = date_str.split(" ")
    month_map = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
        "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
        "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
    }
    month = month_map[date_parts[1]]
    year = date_parts[2][2:]  # Get last two digits of the year
    return f"{month}_{year}"

def extract_text_with_pypdf2(file_path):
    text = ''
    try:
        with open(file_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page_num in range(len(reader.pages)):
                page_text = reader.pages[page_num].extract_text()
                if page_text:
                    text += page_text
    except Exception as e:
        print(f"Failed to extract text from {file_path} using PyPDF2: {e}")
    return text

@app.command()
def download_and_extract_text_for_years(
    start_year: int = typer.Option(2013, help="The start year for the download"),
    end_year: int = typer.Option(2024, help="The end year for the download"),
    output_dir: Path = typer.Argument("docfile", help="The path to the output directory"),
):
    output_dir.mkdir(exist_ok=True)
    combined_csv_path = Path("extracted_texts_all_years.csv")

    # Load existing combined CSV if it exists
    if combined_csv_path.exists():
        combined_df = pd.read_csv(combined_csv_path)
    else:
        combined_df = pd.DataFrame(columns=['filename', 'original_text', 'period', 'period_date'])

    quarters = [
        ('01-01', '03-31'),
        ('04-01', '06-30'),
        ('07-01', '09-30'),
        ('10-01', '12-31')
    ]

    for year in range(start_year, end_year + 1):
        for quarter_start, quarter_end in quarters:
            quarter_name = f"{year}_Q{quarters.index((quarter_start, quarter_end)) + 1}"
            metadata_file = Path(f"metadata_{quarter_name}.csv")
            if not metadata_file.exists():
                typer.echo(f"Metadata file for {quarter_name} does not exist, skipping.")
                continue

            pdf_texts = {}
            pdf_files = []
            with open(metadata_file) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    formatted_date = format_date(row['date'])
                    output_file = output_dir / f"{formatted_date}_{row['tag']}_{row['decision_id']}.pdf"
                    pdf_files.append(output_file)
                    if output_file.exists():
                        typer.echo(f"Skipping {output_file} as it already exists")
                        continue

                    time.sleep(1)
                    decision_url = BASE_DECISIONS_URL + row["location"]
                    urllib.request.urlretrieve(decision_url, output_file)
                    typer.echo(f"Downloaded {output_file}")

                    # Extract text after downloading
                    text = extract_text_with_pypdf2(output_file)
                    pdf_texts[output_file.name] = text

            # Create a DataFrame with the extracted text for the current quarter
            df = pd.DataFrame(list(pdf_texts.items()), columns=['filename', 'original_text'])
            df['period'] = df['filename'].apply(lambda x: re.search(r'(\d{2}_\d{2})', x).group(1) if re.search(r'(\d{2}_d{2})', x) else None)
            df['period_date'] = pd.to_datetime(df['period'], format='%m_%y')

            # Save the DataFrame to a CSV file for the current quarter
            df.to_csv(f"extracted_texts_{quarter_name}.csv", index=False)

            # Delete the PDF files after saving the CSV
            for pdf_file in pdf_files:
                if pdf_file.exists():
                    os.remove(pdf_file)
                    typer.echo(f"Deleted {pdf_file}")

            # Append the new DataFrame to the combined DataFrame
            combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Save the combined DataFrame to the combined CSV file
    combined_df.to_csv(combined_csv_path, index=False)
    typer.echo(f"Saved combined extracted texts to {combined_csv_path}")

if __name__ == "__main__":
    app()
