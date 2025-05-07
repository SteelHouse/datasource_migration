Description

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

Prerequisites
The target taxonomy table must be populated with the target data source category ids and tpa.categories (make sure replication to intprod has already happened) must include the target data source