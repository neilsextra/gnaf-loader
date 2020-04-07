#!/bin/bash

echo "** Launching loader  [2] - dha-poc-01.postgres.database.azure.com  **"

ls -l data

echo "** Listing Complete "

python3 load-gnaf.py --gnaf-tables-path data/GNAF/ --admin-bdys-path data/Administrative_Boundaries/ --pgdb "gnaf-db" --pghost dha-poc-01.postgres.database.azure.com --pguser thesourorange@dha-poc-01 --pgpassword "Mork&Mindy4u"
