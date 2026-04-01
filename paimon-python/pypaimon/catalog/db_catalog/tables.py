################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""SQLAlchemy table definitions matching Java JdbcCatalog DDL exactly.

See: paimon-core/.../jdbc/JdbcUtils.java for the Java DDL.
"""

from sqlalchemy import (
    BigInteger,
    Column,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    text,
)
from sqlalchemy.types import TIMESTAMP

# Shared metadata for all catalog tables
metadata = MetaData()

# paimon_tables — table existence registry
# Java: JdbcUtils.CREATE_CATALOG_TABLE
paimon_tables = Table(
    "paimon_tables",
    metadata,
    Column("catalog_key", String(255), nullable=False),
    Column("database_name", String(255), nullable=False),
    Column("table_name", String(255), nullable=False),
    PrimaryKeyConstraint("catalog_key", "database_name", "table_name"),
)

# paimon_database_properties — database metadata as key/value rows
# Java: JdbcUtils.CREATE_DATABASE_PROPERTIES_TABLE
paimon_database_properties = Table(
    "paimon_database_properties",
    metadata,
    Column("catalog_key", String(255), nullable=False),
    Column("database_name", String(255), nullable=False),
    Column("property_key", String(255)),
    Column("property_value", String(1000)),
    PrimaryKeyConstraint("catalog_key", "database_name", "property_key"),
)

# paimon_distributed_locks — distributed lock table
# Java: AbstractDistributedLockDialect.getCreateTableSql()
# Note: lock_key_max_length defaults to 255, matching Java's JdbcCatalogOptions.LOCK_KEY_MAX_LENGTH
paimon_distributed_locks = Table(
    "paimon_distributed_locks",
    metadata,
    Column("lock_id", String(255), nullable=False),
    Column("acquired_at", TIMESTAMP, nullable=False, server_default=text("CURRENT_TIMESTAMP")),
    Column("expire_time_seconds", BigInteger, nullable=False, server_default=text("0")),
    PrimaryKeyConstraint("lock_id"),
)
