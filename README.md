# Schema management

Customers are able to send us new columns and have them be immediately queryable. However, we also want to support an relational columnar store for doing deep analytics, so we need to keep track of schema to periodically update the analytics store's schema and allow querying over the columns.

To be immediately queryable, we also need to keep track of new column names as they appear and, potentially, be able to alias them under an existing column. This allows customers to bring new machines online that export differently named metrics, but have near-zero overhead to use this new column name along side the original column.

For more information, see the in-depth [schema management doc](https://docs.google.com/document/d/1SchdbCqHZsSdgIaopNqZh1xtnLB-KYcy-anCxyotiOQ/edit#).

## Schema Repository

The changing schema is stored and accesed in one particular place - the Schema Repository. This repository allows us to manage schema changes over time and reference schemas by id.

The repository is built around the [open source schema-repo](https://github.com/schema-repo/schema-repo).

### Accessing the Repository

Schema-Repo is built around a REST client for a schema repository backend. However, we don't really want to run and manage another service (eventually!). For now, we leverage a local schema adapter that looks like the client, but directly accesses the schema repository backend.

This has the added advantage of being lower latency, at the cost of a loss of abstraction. Thus, we can get into cases where the schema changes for storing the schema and we need to support the update. For now, that's ok because schema storage change should be rare and we are smart enough to support backwards compatible schemas.

#### Repository

The repository is built on DynamoDB. This is merely out of convenience of having that database already present in the infrastructure (again, not wanting to run too many services), not because we need the inherent scale. In fact, the eventually consistent model is suboptimal for our use case as we would like to have easily understood, step-wise schema evolution; we avoid that by using consistent reads.

Honestly, schema shouldn't be that large or changing super frequently - a traditional, single node RDBMS would be more than enough for quite a while ($$$ is tough).

###### Security

Schema is just as important as the actual data and must be encrypted at all times - on the wire, at rest and backups.

Schema changes are monitored just as any other access to the platform (through the realtime stream, stored in NODS and CAS). This means we can monitor and audit all schema changes.

###### Backups

Since schema changes are proliferated through the platform, we can also set them aside in S3 for a special set of backups. Schema backups are different from other events because they need to be replayed differently.

