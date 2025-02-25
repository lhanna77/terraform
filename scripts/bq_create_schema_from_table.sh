#!/bin/bash
gcloud config set project mstr-globalbi-sbx-c730
bq.cmd show --schema --format=prettyjson mstr-dwh-dev-83bf:lh_sfmc_export_spike_US.lookup_EmployerUserEventsHistory > table.json


