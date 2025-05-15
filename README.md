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

The tool provides several commands:

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

3. Get Current Sizes:
   ```bash
   poetry run get-sizes
   ```
   This command:
   - Fetches current audience sizes for all data sources
   - Stores the size data in `match_finder/current_cat_sizes/data.json`
   - DOES NOT NEED TO BE CALLED DIRECTLY

4. Enrich with Sizes:
   ```bash
   poetry run enrich-sizes
   ```
   This command:
   - Calls Get Current Sizes to pull size file
   - Enriches the match data with audience size information
   - Processes both `similarity_too_low/data.csv` and `suggested_match/data.csv`
   - Adds size information and calculates size changes
   - Creates new files with `_with_size.csv` suffix in the same directories

5. Enrich Mapping with Sizes:
   ```bash
   poetry run enrich-mapping <source_id> <target_id> <mapping_file>
   ```
   This command:
   - Calls Get Current Sizes to pull size file
   - Takes a specific mapping file from the `mapping_files` directory
   - Enriches it with audience size information from both source and target data sources
   - Creates a new file in the `qa_size` directory with timestamp prefix
   - Shows statistics about size changes between source and target audiences
   Example:
   ```bash
   poetry run enrich-mapping 18 35 dstillery-to-lr-mapping.csv
   ```

6. Find and Enrich:
   ```bash
   # Use default similarity threshold (0.60)
   poetry run find-and-enrich

   # Or specify a custom similarity threshold (between 0 and 1)
   poetry run find-and-enrich 0.75
   ```
   This command:
   - Combines the functionality of `find-matches` and `enrich-sizes`
   - First finds matches using the specified similarity threshold
   - Then automatically enriches the results with size information
   - Produces both match files and their size-enriched versions in one step

### Typical Workflow

1. Prepare your data files using the SQL queries above
2. Place the files in their respective directories:
   - `match_finder/needs_match/data.csv`
   - `match_finder/total_match_options/data.csv`
3. Run `poetry run find-and-enrich [threshold]` to generate matches and size data
   - Optionally specify a similarity threshold (default is 0.60)
   - Higher threshold = stricter matching
   - Lower threshold = more permissive matching
4. Review the matches and size data:
   - Check `suggested_match/data_with_size.csv` for good matches and their audience sizes
   - Review `similarity_too_low/data_with_size.csv` for potential manual review
   - Pay attention to significant size differences between source and target audiences
5. If satisfied with the matches and size comparisons, run `poetry run create-mapping <name>` to create the final mapping file
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



