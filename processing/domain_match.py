import pandas as pd
import re
from rapidfuzz import fuzz, process

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = name.lower()
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    name = re.sub(r'\b(pty|ltd|limited|australia|australian|company|inc|co)\b', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def domain_root(domain):
    domain = domain.lower()
    domain = re.sub(r'^www\.', '', domain)
    parts = domain.split('.')
    if len(parts) > 2:
        return parts[-3]  # take root part before .com.au
    return parts[-2]

# --- load data ---
abr = pd.read_csv("/path/tocsv" , low_memory=False ,   nrows=1_000_000 )
cc = pd.read_csv("path/tocsv")

# normalize names
abr["Entity_Name_norm"] = abr["Entity_Name"].apply(normalize_name)
abr["Trading_Names_norm"] = abr["Trading_Names"].apply(normalize_name)
abr["all_names_norm"] = abr["Entity_Name_norm"] + " " + abr["Trading_Names_norm"]

cc["domain_root"] = cc["domain"].apply(domain_root)
cc["title_norm"] = cc["meta"].str.extract(r"'title': '([^']+)'")[0].fillna("").apply(normalize_name)

matches = []
for _, cc_row in cc.iterrows():
    candidate = cc_row["domain_root"]

    match = process.extractOne(
        candidate,
        abr["all_names_norm"],
        scorer=fuzz.token_sort_ratio
    )

    if match:
        best_abn, score, _ = match
        if score >= 90:
            abn_row = abr.loc[abr["all_names_norm"] == best_abn].iloc[0]
            matches.append({
                "domain": cc_row["domain"],
                "url": cc_row["url"],
                "abn": abn_row["ABN"],
                "entity_name": abn_row["Entity_Name"],
                "trading_name": abn_row["Trading_Names"],
                "score": score
            })
        else:
            matches.append({
                "domain": cc_row["domain"],
                "url": cc_row["url"],
                "abn": None,
                "entity_name": None,
                "trading_name": None,
                "score": score
            })

result = pd.DataFrame(matches)
result.to_csv("domain_to_abn_matches.csv", index=False)
# print(result.head(20))
