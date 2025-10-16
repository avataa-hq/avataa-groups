# Group Builder

## Environment variables

```toml
DB_HOST=<pgbouncer/postgres_host>
DB_NAME=<pgbouncer/postgres_group_builder_db_name>
DB_PASS=<pgbouncer/postgres_group_builder_password>
DB_PORT=<pgbouncer/postgres_port>
DB_TYPE=postgresql+asyncpg
DB_USER=<pgbouncer/postgres_group_builder_user>
DOCS_CUSTOM_ENABLED=<True/False>
DOCS_REDOC_JS_URL=<redoc_js_url>
DOCS_SWAGGER_CSS_URL=<swagger_css_url>
DOCS_SWAGGER_JS_URL=<swagger_js_url>
INVENTORY_GRPC_PORT=<inventory_grpc_port>
INVENTORY_HOST=<inventory_host>
KAFKA_BUFFER_TIMEOUT_SEC=15
KAFKA_GROUP_DATA_CHANGES_PROTO=group_data.changes
KAFKA_INVENTORY_TOPIC=inventory.changes
KAFKA_OFFSET=latest
KAFKA_TOPIC=group
KAFKA_URL=<kafka_host>:<kafka_port>
KAFKA_WITH_KEYCLOAK=<True/False>
KAFKA_SASL_MECHANISM=<kafka_sasl_mechanism>
KAFKA_SECURITY_PROTOCOL=<kafka_security_protocol>
KEYCLOAK_CLIENT_ID=<kafka_client>
KEYCLOAK_CLIENT_SECRET=<kafka_client_secret>
KEYCLOAK_HOST=keycloak
KEYCLOAK_PORT=8080
KEYCLOAK_PROTOCOL=http
KEYCLOAK_REALM=avataa
KEYCLOAK_REDIRECT_HOST=<keycloak_external_host>
KEYCLOAK_REDIRECT_PORT=<keycloak_external_port>
KEYCLOAK_REDIRECT_PROTOCOL=<keycloak_external_protocol>
KEYCLOAK_SCOPE=profile
LOGGING=<logging_level>
REDIS_HOST=<redis_host>
REDIS_PORT=<redis_port>
SEARCH_CLIENT_HOST=search
SEARCH_GRPC_PORT=50051
SECURITY_TYPE=<security_type>
UVICORN_WORKERS=<uvicorn_workers_number>
```

### Explanation

#### API
`INVENTORY_HOST` API Inventory host (default: _localhost_)
`SEARCH_CLIENT_HOST` API MS Search client host (default: _localhost_)

#### Common
`LOGGING` Level of logging (default: _50_)
`LOG_WITH_TIME` Add time to logger (default: _False_)
`DOCS_ENABLED` Enable docs (default: _True_)
`DOCS_CUSTOM_ENABLED` Enable custom docs (default: _False_)
`DOCS_SWAGGER_JS_URL` Swagger JS URI (default: _""_)
`DOCS_SWAGGER_CSS_URL` Swagger CSS URI (default: _""_)
`DOCS_REDOC_JS_URL` Redoc JS URI (default: _""_)

#### Database
`DB_TYPE` Type of database (default: _postgresql+asyncpg_)
`DB_USER` Pre-created user in the database with rights to edit the database (default: _group_builder_admin_)
`DB_PASS` Database user password  (default: _pass_)
`DB_HOST` Database host (default: _localhost_)
`DB_PORT` Database port (default: _5432_)
`DB_NAME` Database name (default: _group_builder_)
`DB_SCHEMA` SQL Schema name (default: _public_)

#### GRPC
`INVENTORY_GRPC_PORT` Inventory gRPC server port (default: _10000_)
`SEARCH_GRPC_PORT` Search MS gRPC server port (default: _10000_)
`SERVER_GRPC_PORT` gRPC server port (default: _50051_)

#### Kafka
`KAFKA_TURN_ON` Enable kafka (default: _True_)
`KAFKA_SECURED` Activate Keycloak config (default: _True_)
`KAFKA_TOPIC` Kafka topic for data about groups (default: _group_topic_)
`KAFKA_GROUP_DATA_CHANGES_PROTO` Kafka topic for statistic (default: _group_changes_topic_)
`KAFKA_INVENTORY_TOPIC` Kafka inventory topic (default: _inventory_topic_)
`KAFKA_URL` Kafka address for bootstratp servers (default: _kafka:9092_)
`KAFKA_CONSUMER_NAME` Kafka consumer group id (default: _Group_consumer_)
`KAFKA_OFFSET` Offset for Kafka (default: _earliest_)
`KAFKA_SECURITY_PROTOCOL` Kafka security protocol (default: _None_)
`KAFKA_SASL_MECHANISM` Kafka sasl mechanism (default: _None_)

#### Keycloak
`KEYCLOAK_PROTOCOL` Keycloak protocol (default: _https_)
`KEYCLOAK_HOST` Keycloak protocol (default: _keycloak_)
`KEYCLOAK_PORT` Keycloak protocol (default: _443_)
`KEYCLOAK_REALM` Keycloak protocol (default: _example_)
`KEYCLOAK_CLIENT_ID` Keycloak client id (default: _kafka_)
`KEYCLOAK_CLIENT_SECRET` Keycloak client secret (default: _secret_)

#### Redis
`REDIS_HOST` Redis address (default: _redis_)
`REDIS_PORT` Redis port (default: _6379_)
`REDIS_PASS` Redis password (default: _password_)

#### Auto group
`KAFKA_BUFFER_TIMEOUT_SEC` Buffer timeout for checking changes in the auto group (default: _15_)

#### Compose

- `REGISTRY_URL` - Docker regitry URL, e.g. `harbor.avataa.dev`
- `PLATFORM_PROJECT_NAME` - Docker regitry project Docker image can be downloaded from, e.g. `avataa`

### Requirements
```
python 3.11
$ uv sync
```

### Running

```
$ cd app
$ uvicorn main:app --reload

INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [28720]
INFO:     Started server process [28722]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

### Testing
Required modules
- pytest
- pytest-asyncio
- aiosqlite
