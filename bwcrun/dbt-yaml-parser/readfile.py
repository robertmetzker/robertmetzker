import os
from pathlib import Path

filename  = Path("C:/Users/rmetz/Downloads/repo-dbt-snowflake/repo-dbt-snowflake/yamls/tests.yml")

with open(filename) as f:
    for line in f:
        print( line )
