import pandas as pd
import re
import ftfy
from unidecode import unidecode
from thefuzz import process
from tqdm import tqdm
from functools import lru_cache

# Optional: enable parallel processing using pandarallel
# Uncomment these lines if you want to use pandarallel for further speedup.
# from pandarallel import pandarallel
# pandarallel.initialize(progress_bar=True)

# Initialize tqdm for pandas
tqdm.pandas()

# Load CSV
print("📂 Loading CSV file...")
df = pd.read_csv("omr_all_reviews.csv")
print("✅ CSV file loaded.")

# Pre-compile common regex patterns.

# Remove website components pattern (protocol, www, and common TLDs)
WEBSITE_REGEX = re.compile(r"(https?:\/\/)?(www\.)?|(\.com|\.ch|\.de|\.net|\.org|\.eu|\.io|\.biz|\.co|\.uk)$", re.IGNORECASE)

# Define common suffixes & compile regexes for them.
REMOVE_TERMS = [" ag", " gmbh", " inc", " ltd", " s.a.", " co.", " corporation", " llc"]
REMOVE_PATTERNS = [re.compile(r"\b" + term.strip() + r"\b", re.IGNORECASE) for term in REMOVE_TERMS]

# Pre-compile punctuation cleaning regex.
PUNCTUATION_REGEX = re.compile(r"[^a-z0-9\s&]")  # Keep alphanumeric, spaces, and ampersand.
MULTISPACE_REGEX = re.compile(r"\s+")

def clean_company_name(name):
    """
    Cleans a company name by fixing encoding issues, normalizing Unicode,
    removing website patterns, common company suffixes, extra punctuation,
    and extra whitespace.
    """
    if pd.isna(name) or name.strip() == "":
        return None  # Mark for deletion if empty

    # Fix encoding issues and normalize to ASCII.
    name = ftfy.fix_text(name)
    name = unidecode(name)
    
    # Lowercase and strip whitespace.
    name = name.lower().strip()

    # Remove website components.
    name = WEBSITE_REGEX.sub("", name)

    # Remove common company suffixes.
    for pattern in REMOVE_PATTERNS:
        name = pattern.sub("", name)

    # Remove extra punctuation.
    name = PUNCTUATION_REGEX.sub(" ", name)
    
    # Replace multiple spaces with a single space.
    name = MULTISPACE_REGEX.sub(" ", name).strip()
    
    return name if name else None

print("🧹 Cleaning company names...")
# Use parallel_apply if pandarallel is enabled; otherwise, use progress_apply.
# For example, if using pandarallel, replace .progress_apply with .parallel_apply.
df["cleaned_company"] = df["company"].astype(str).progress_apply(clean_company_name)
# If using pandarallel, you could do:
# df["cleaned_company"] = df["company"].astype(str).parallel_apply(clean_company_name)

# Remove invalid companies.
df = df.dropna(subset=["cleaned_company"])
print("✅ Company names cleaned.")

# Get unique company names.
unique_companies = df["cleaned_company"].unique()

@lru_cache(maxsize=None)
def cached_match_company(name):
    """
    Uses fuzzy matching to pick the best match among known companies.
    Uses caching to avoid repeated computation for the same company name.
    """
    best_match, score = process.extractOne(name, unique_companies)
    return best_match if score > 85 else name

def match_company(name):
    return cached_match_company(name)

print("🔍 Matching similar company names with fuzzy matching...")
df["matched_company"] = df["cleaned_company"].progress_apply(match_company)
print("✅ Fuzzy matching completed.")

# Optional: additional DataFrame-wide cleaning for all text columns.
def clean_text_columns(dataframe):
    """
    Iterates through all object (string) columns in the DataFrame and applies
    generic text cleaning: fixing encoding, normalizing Unicode, lowercasing,
    and trimming spaces.
    """
    for col in dataframe.select_dtypes(include=["object"]).columns:
        dataframe[col] = dataframe[col].progress_apply(lambda x: unidecode(ftfy.fix_text(x)) if isinstance(x, str) else x)
        dataframe[col] = dataframe[col].str.strip()
    return dataframe

# Uncomment the next line if you want to clean all text columns.
# df = clean_text_columns(df)

print("💾 Saving cleaned data to CSV...")
df.to_csv("cleaned_reviews.csv", index=False)
print("✅ Company names cleaned, matched, and unknown companies removed successfully!")
print("✅ Cleaned CSV saved as 'cleaned_reviews.csv'.")