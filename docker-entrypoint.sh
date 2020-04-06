#!/bin/bash

echo "** Launching loader  [2] - dha-poc-01.postgres.database.azure.com  **"

ls -l /data

echo "** Listing Complete "

python load-gnaf.py --gnaf-tables-path <gnaf-data> --admin-bdys-path <administrative-boundaries> "gnaf-db" --pghost <hostname>.postgres.database.azure.com --pguser <userid>@<hostname> --pgpassword <password>
