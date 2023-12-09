#!/bin/bash

sh_dataset=LookupTables
sh_table=BudgetRates
sh_schema=budget_rates.json
sh_from_file_path="files/json/$sh_table.json"
sh_bq_table="dev_$sh_table"

# Create schema

py -3.9 lhannah_bq_get_table_schema.py -p mstr-dwh-dev-83bf -d $sh_dataset -t $sh_table -s $sh_schema

echo Create tf Table Sleep Start - $(date)!
sleep 120
echo Create tf Table Complete - $(date)!

# Populate topics

# py -3.9 lhannah_ps_df_test_populate_topic.py;
py -3.9 lhannah_ps_df_test_populate_topic.py -t lhannah-ps-df-test-topic-file -sif True -fp $sh_from_file_path;

# echo Topic Loads Complete - $(date)!

# # Populate tables from subscriptions

# py -3.9 lhannah_ps_df_test_load_df.py -sif True \
#     -fp files/json/DataControl.json \
#     -t dev_DataControl \
#     -s bqschemas/data_control.json

# echo Table Loads from File Complete - $(date)!

# # Sleep for subs to populate

echo Sub Populate Sleep Start - $(date)!
sleep 180
echo Sub Populate Sleep Complete - $(date)!

# # Populate tables from subscriptions

py -3.9 lhannah_ps_df_test_load_df.py -sif False \
    -sub lhannah-ps-df-test-sub-file \
    -t $sh_bq_table \
    -s bqschemas/sub_to_bq_schema/$sh_schema #& \
 
# # py -3.9 lhannah_ps_df_test_load_df.py -sif False \
# #     -sub lhannah-ps-df-test-sub \
# #     -t dev_LhannahPsDfTest \
# #     -s bqschemas/lhannah_ps_df_test.json &

# echo Table Loads from Subscriptions Complete - $(date)!

# # py -3.9 lhannah_ps_df_test_load_df.py --streaming \
# # --runner DataflowRunner \
# # --project mstr-globalbi-sbx-c730 \
# # --region us-east1 \
# # --temp_location gs://lhannah-sink-dev/df/ \
# # --job_name lhannah-ps-df-test-dataflow \
# # --max_num_workers 2 \
# # --input_subscription projects/mstr-globalbi-sbx-c730/subscriptions/lhannah-ps-df-test-sub-file \
# # --output_table mstr-globalbi-sbx-c730:dev_lhannah_dataset_test.dev_DataControl \
# # --output_schema parse_table_schema_from_json("data_control_pub_sub")