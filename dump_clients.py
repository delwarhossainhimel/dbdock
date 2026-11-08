"""
Registry of supported dump clients per database type.
Add new entries here whenever you install a new dump binary.
"""

MYSQL_DUMP_CLIENTS = [
    {
        'value': 'mysqldump56',
        'label': 'mysqldump56 (MySQL 5.6 / MariaDB)',
        'description': 'Use for MySQL/MariaDB 5.6 and older',
        'options': [
            '--single-transaction',
            '--skip-lock-tables',
            '--skip-add-locks',
            '--routines',
            '--triggers',
            '--add-drop-database',
            '--databases',
        ],
    },
    {
        'value': 'mysqldump80',
        'label': 'mysqldump80 (MySQL 8.0)',
        'description': 'Use for MySQL 8.0 servers',
        'options': [
            '--single-transaction',
            '--skip-lock-tables',
            '--skip-add-locks',
            '--routines',
            '--triggers',
            '--add-drop-database',
            '--databases',
        ],
    },
    {
        'value': 'mysqldump84',
        'label': 'mysqldump84 (MySQL 8.4)',
        'description': 'Use for MySQL 8.4 servers',
        'options': [
            '--single-transaction',
            '--skip-lock-tables',
            '--skip-add-locks',
            '--routines',
            '--triggers',
            '--add-drop-database',
            '--databases',
        ],
    },
    {
        'value': 'mysqldump',
        'label': 'mysqldump (MySQL latest)',
        'description': 'Use for MySQL newer',
        'options': [
            '--single-transaction',
            '--skip-lock-tables',
            '--skip-add-locks',
            '--routines',
            '--triggers',
            '--add-drop-database',
            '--databases',
        ],
    },
]

POSTGRES_DUMP_CLIENTS = [
    {
        'value': 'pg_dump',
        'label': 'pg_dump (system default)',
        'description': 'Uses the pg_dump found on PATH',
    },
    # {
    #     'value': 'pg_dump12',
    #     'label': 'pg_dump12 (PostgreSQL 12)',
    #     'description': 'Use for PostgreSQL 12 servers',
    # },
    # {
    #     'value': 'pg_dump13',
    #     'label': 'pg_dump13 (PostgreSQL 13)',
    #     'description': 'Use for PostgreSQL 13 servers',
    # },
    # {
    #     'value': 'pg_dump14',
    #     'label': 'pg_dump14 (PostgreSQL 14)',
    #     'description': 'Use for PostgreSQL 14 servers',
    # },
    {
        'value': 'pg_dump15',
        'label': 'pg_dump15 (PostgreSQL 15)',
        'description': 'Use for PostgreSQL 15 servers',
    },
    {
        'value': 'pg_dump16',
        'label': 'pg_dump16 (PostgreSQL 16)',
        'description': 'Use for PostgreSQL 16 servers',
    },
    {
        'value': 'pg_dump17',
        'label': 'pg_dump17 (PostgreSQL 17)',
        'description': 'Use for PostgreSQL 17 servers',
    },
]


def get_dump_clients_for_type(db_type: str):
    """Return list of dump clients for 'mysql' or 'postgres'."""
    if db_type == 'mysql':
        return MYSQL_DUMP_CLIENTS
    elif db_type == 'postgres':
        return POSTGRES_DUMP_CLIENTS
    return []


def get_default_dump_client(db_type: str) -> str:
    """Return default dump client value for a db type."""
    if db_type == 'mysql':
        return 'mysqldump84'
    elif db_type == 'postgres':
        return 'pg_dump'
    return None


def get_dump_client_options(db_type: str, client_value: str):
    """Return the CLI options for a given dump client."""
    for client in get_dump_clients_for_type(db_type):
        if client['value'] == client_value:
            return client.get('options', [])
    return []


def is_valid_dump_client(db_type: str, client_value: str) -> bool:
    """Validate that the client value is allowed for the db type."""
    return any(
        c['value'] == client_value
        for c in get_dump_clients_for_type(db_type)
    )