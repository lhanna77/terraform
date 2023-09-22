#!/bin/bash
gcloud config set project mstr-globalbi-sbx-c730
bq.cmd update mstr-globalbi-sbx-c730:dev_lhannah_dataset_test.dev_JobAppliesCurrent bqschemas/job_applies_current_policy_tags.json


