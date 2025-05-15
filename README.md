# Data Source Migration Tools

This repository contains tools for mapping and migrating audience data between different data sources.

## Prerequisites
The target taxonomy table must be populated with the target data source category ids and tpa.categories (make sure replication to intprod has already happened) must include the target data source

Instructions

Add a file named `pass.ini` to the root of the project with the following contents adding passwords as needed (liveramp_authorization is not needed at the moment pending implementation):

```ini
[redshift_coredw]
password=
[qacoredb]
password=
[integrationprod]
password=
[liveramp_authorization]
password=
```

## Installation

1. Make sure you have Python 3.10 or newer installed
2. Install Poetry if you haven't already:
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```
3. Clone this repository
4. Run `poetry install` in the repository root

## Match Finder Tool

The Match Finder tool helps you find semantic matches between audience segments from different data sources. It uses TF-IDF and cosine similarity to find the best matches based on segment names.

### Data Preparation

You'll need two CSV files with audience segment data:

1. `match_finder/needs_match/data.csv` - Segments that need matches
2. `match_finder/total_match_options/data.csv` - All possible segments to match against

You can generate these files using SQL queries on your database. For example:

```sql
-- For needs_match/data.csv (replace YOUR_ID with source data source ID)
select data_source_id,data_source_category_id,name 
from tpa.categories
where data_source_id = YOUR_ID;

-- For total_match_options/data.csv (use target data source ID)
select data_source_id,data_source_category_id,name 
from tpa.categories
where data_source_id = TARGET_ID;
```

Each CSV file should have three columns in this order:
- data_source_id
- data_source_category_id
- name

### Available Commands

The tool provides two main commands:

1. Find Matches:
   ```bash
   # Use default similarity threshold (0.60)
   poetry run find-matches

   # Or specify a custom similarity threshold (between 0 and 1)
   poetry run find-matches 0.75
   ```
   This command:
   - Reads segments from both data files
   - Uses TF-IDF and cosine similarity to find the best matches
   - Splits results based on similarity threshold:
     - Matches above threshold → `match_finder/suggested_match/data.csv`
     - Matches below threshold → `match_finder/similarity_too_low/data.csv`
   - Includes similarity scores to help evaluate match quality

2. Create Mapping File:
   ```bash
   poetry run create-mapping <name>
   ```
   This command:
   - Takes the matches from `suggested_match/data.csv`
   - Creates a mapping file in `mapping_files/<name>.csv`
   - The mapping file contains just the category IDs needed for migration:
     - origin_data_source_category_id
     - target_data_source_category_id

### Typical Workflow

1. Prepare your data files using the SQL queries above
2. Place the files in their respective directories:
   - `match_finder/needs_match/data.csv`
   - `match_finder/total_match_options/data.csv`
3. Run `poetry run find-matches [threshold]` to generate matches
   - Optionally specify a similarity threshold (default is 0.60)
   - Higher threshold = stricter matching
   - Lower threshold = more permissive matching
4. Review the matches:
   - Check `suggested_match/data.csv` for good matches
   - Review `similarity_too_low/data.csv` for potential manual review
5. If satisfied with the matches, run `poetry run create-mapping <name>` to create the final mapping file
6. Find your mapping file in `mapping_files/<name>.csv`

### Notes

- Similarity scores range from 0 to 1:
  - 1.0 = perfect match
  - >0.8 = very good match
  - >0.6 = decent match (default threshold)
  - <0.5 = weak match (may need manual review)
- The matcher uses semantic similarity, so it can match segments even when names aren't exactly the same
- Always review the matches before using them in production
- Consider adjusting the similarity threshold based on your needs:
  - Increase for higher confidence matches
  - Decrease if you're missing too many potential matches

Description

**NOTE: This script does not handle version 2 audience expressions!**

map_dscids_to_new_datasource.py maps origin data source category ids for a given data source to target data source category ids in audiences, then reapplies the audiences.
All origin data source category ids are marked deprecated in the taxonomy tables.

Optionally, if the origin data source is Liveramp, it can remove segments from Liveramp distribution and remove origin LiveRamp provider from automated Liveramp updates

Inputs (Set GLOBAL VARIABLES)
1. CSV file in the mapping_files folder
   The file must contain the dscid mapping from the origin data source to the target data source and the following
   column headers:
   origin_data_source_category_id
   target_data_source_category_id
2. CSV file name
3. Origin data source id
4. Target data source id



