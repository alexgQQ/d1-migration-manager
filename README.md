
# Manage Migrations and Data Replication with D1 Databases

This is a tool made for my use case but could be helpful to you. It sets up some migration management for a local D1 database so schema and data changes can be created in a safe and sequential manner. It is both a way to handle DDL and data replication for small databases with isolated write operations. If you manage your SQL data for D1 locally then give it a spin. Otherwise it contains some interesting functionality for database triggers, auditing and change operations. Check out the [repo](https://github.com/alexgQQ/d1-migration-manager).

## Install

This module only requires Python 3.12+. To install:
```bash
git clone https://github.com/alexgQQ/d1-migration-manager
cd d1-migration-manager
pip install .
```
To uninstall:
```bash
pip uninstall d1_migration_manager
```

## Usage

At it's core this module just interacts with a sqlite database and generates sql migration files. You'll need a sqlite database to use and a place to track these files. It is intended to be used with [wrangler](https://developers.cloudflare.com/workers/wrangler/) and a [D1](https://developers.cloudflare.com/d1/) instance. You can use this against a local copy of your D1 database located in your project `.wrangler/state/db/v3/d1/` and the migration directory specified in it's configuration.

This will setup your local instance with auditing so any changes can be pushed through migrations to your remote instance via [`wrangler d1 migrations apply`](https://developers.cloudflare.com/workers/wrangler/commands/#d1-migrations-apply).

**This is intended for small databases under 5GB with locally isolated write operations**


The cli interface is available as a module entrypoint. Make sure the module works and check out the usage text.
```bash
python -m d1_migration_manager --version
python -m d1_migration_manager --help
```

Symlink the local wrangler d1 instance for easier usage. Update or skip this if persisting it elsewhere.
```bash
ln -s .wrangler/state/db/v3/d1/miniflare-D1DatabaseObject/*.sqlite target_db.sqlite
```

For the first time use on a database you'll need to generate an initial migrations and then apply the audit triggers.
```bash
python -m d1_migration_manager -db target_db.sqlite -dir ./migrations --initial
python -m d1_migration_manager -db target_db.sqlite --track
```
This creates a sql dump for the first migration then adds a table to track change events with triggers to populate it on any INSERT, UPDATE or DELETE statement.
If you already have some migration files then just setup the database for auditing.
```bash
python -m d1_migration_manager -db target_db.sqlite --track
```

Now you can check if any new data changes have been applied. This reads from the latest migration file and finds any changes since then.
```bash
python -m d1_migration_manager -db target_db.sqlite -dir ./migrations --check
```
Then generate a sql file to represent those data changes. Each migration after the initial need a specified message.
```bash
python -m d1_migration_manager -db target_db.sqlite -dir ./migrations -m "added some data since 04-25"
```

The change events **do not** track schema changes. Those have to be manually written and triggers **must** be reapplied after the schema change.
It is also important to note that schema and data changes **must** be separated. If data changes are present then this will error and you must generate data migrations before continuing.
```bash
python -m d1_migration_manager -db target_db.sqlite -dir ./migrations --schema -m "alter table 04-26"
```
Write the DDL and apply it. If all checks out then reapply the audit triggers.
```bash
python -m d1_migration_manager -db target_db.sqlite -dir ./migrations --track
```

If you ever need to disable the auditing the triggers can be removed
```bash
python -m d1_migration_manager -db target_db.sqlite --untrack
```


### Importing

This is also made to be used in code. The top level functionality can be imported and used however you'd like. This example will make a data migration file for anything since last week.
```python
import sqlite
from datetime import datetime, UTC, timedelta

from d1_migration_manager import create_data_migration


database = "my_db.sqlite"
migrations_dir = "migrations"
since_last_week = datetime.now(UTC) - timedelta(weeks=1)
db = sqlite3.connect(database)
migration_filepath = create_data_migration(db, migrations_dir, "since last week", 1, since_last_week)
print(migration_filepath)
``` 

### Development

Make a venv to work in. The core module has no dependencies but there are some dependencies for development and building the docs. 
```bash
make env
source .venv/bin/activate
pip install -e .[dev]
```
Keep it clean and functional
```bash
make fmt
make test
```

### Building Docs

This uses Sphinx to generate documentation. Its very simple and just contains the README and reference docs for the top level functions generated from their docstrings.
The `_build` directory contains the generated assets.
```bash
pip install -e .[doc]
cd docs
make html
python -m http.server -d _build/html 8080
```
It can be viewed on localhost:8080. This can reflect changes in place.
```bash
make clean && make html
```
And refresh the page.

