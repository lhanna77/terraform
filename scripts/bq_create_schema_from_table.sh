#!/bin/bash
gcloud config set project mstr-globalbi-sbx-c730
bq.cmd show --schema --format=prettyjson mstr-fivetran-stage-dev-3b44:zuora.account > bqschemas/zuora_account.json


