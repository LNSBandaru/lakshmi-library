1. Choose which cluster(s) need CDC
    Enatch cluster needed:
        A custom cluster Parameter Group (one)
        A custom Instance Paramater Group (read - write)


Logical Replication Enabled

2. Create Cluster parameter group

aws rds create-db-cluster-parameter-group \
  --db-cluster-parameter-group-name catalog-pg-cdc-cluster \
  --db-parameter-group-family aurora-postgresql16 \
  --description "CDC Logical Replication (catalog cluster)"


3. Instance Paramer group

aws rds create-db-parameter-group \
  --db-parameter-group-name catalog-pg-cdc-instance \
  --db-parameter-group-family aurora-postgresql16 \
  --description "CDC Instance Parameter Group (catalog)"

4. Set logical replication parameters

aws rds modify-db-cluster-parameter-group \
  --db-cluster-parameter-group-name catalog-pg-cdc-cluster \
  --parameters \
    "ParameterName=rds.logical_replication,ParameterValue=1,ApplyMethod=pending-reboot" \
    "ParameterName=max_replication_slots,ParameterValue=10,ApplyMethod=pending-reboot" \
    "ParameterName=max_wal_senders,ParameterValue=10,ApplyMethod=pending-reboot"

5. attach parameter groups  [Cluster DB] & [Instance DB (Read & Write)]

    Cluster level PG [1]

aws rds modify-db-cluster \
  --db-cluster-identifier cat-catalog-postgres-dev \
  --db-cluster-parameter-group-name catalog-pg-cdc-cluster \
  --apply-immediately

    Instance level PG [2] 

aws rds modify-db-instance \
  --db-instance-identifier cat-catalog-postgres-dev-1 \
  --db-parameter-group-name catalog-pg-cdc-instance \
  --apply-immediately

aws rds modify-db-instance \
  --db-instance-identifier cat-catalog-postgres-dev-2 \
  --db-parameter-group-name catalog-pg-cdc-instance \
  --apply-immediately

6. Reboot (Reader -> Failover pattern)

    Step A: Reboot a reader first
aws rds reboot-db-instance \
  --db-instance-identifier cat-catalog-postgres-dev-1 \
  --region us-east-1

aws rds reboot-db-instance \
  --db-instance-identifier cat-catalog-postgres-dev-2 \
  --region us-east-1

    Step B: Verify

SHOW rds.logical_replication;
SHOW wal_level;

    Step C: Fail over cluste

aws rds failover-db-cluster \
  --db-cluster-identifier cat-catalog-postgres-dev \
  --region us-east-1


7. Create CDC use

CREATE ROLE catalog_cdc WITH LOGIN PASSWORD 'o_GaHYy!aRreCk9k9Q0BPzi9!';
GRANT rds_replication, rds_superuser TO catalog_cdc;
CREATE PUBLICATION openflow_publication FOR ALL TABLES;









