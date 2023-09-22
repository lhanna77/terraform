#!/bin/bash
gcloud config set project mstr-globalbi-sbx-c730
bq.cmd show --schema --format=prettyjson mstr-sevenseas-dev-cd2c:dl_jobapplies.current_v2 > bqschemas/job_applies_current.json