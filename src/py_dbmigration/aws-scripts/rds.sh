#!/bin/bash
#https://docs.aws.amazon.com/cli/latest/reference/rds/create-db-instance.html
#https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.html
aws rds create-db-instance \
--allocated-storage 20 --db-instance-class db.t2.small \
--db-instance-identifier test-instance-maria \
--engine mariadb \
--enable-cloudwatch-logs-exports '["audit","error","general","slowquery"]' \
--no-deletion-protection \
--master-username master --master-user-password secret99

aws rds delete-db-instance \
    --skip-final-snapshot \
    --db-instance-identifier test-instance
    
aws rds create-db-instance \
--allocated-storage 20 --db-instance-class db.t2.small \
--db-instance-identifier test-instance-postgres2 \
--engine postgres \
--enable-cloudwatch-logs-exports '["postgresql","upgrade"]' \
--no-deletion-protection --backup-retention-period 0 \
--master-username master \
--master-user-password secret99 --port 5432

aws rds delete-db-instance \
    --skip-final-snapshot \
    --db-instance-identifier test-instance-postgres
    
# aws rds create-db-instance \
# --allocated-storage 20 --db-instance-class db.t2.medium \
# --db-instance-identifier test-instance-sqlserver2 \
# --engine sqlserver-ee \
# --enable-cloudwatch-logs-exports   \
# --license-model license-included  \
# --no-deletion-protection \
# --master-username master --master-user-password secret99



aws rds create-db-instance \
    --engine sqlserver-web \
    --db-instance-identifier test-instance-sqlserver \
    --allocated-storage 30 \
    --db-instance-class db.t2.medium \
    --master-username master \
    --master-user-password secret99 \
    --backup-retention-period 0

aws rds delete-db-instance \
    --skip-final-snapshot \
    --db-instance-identifier test-instance-sqlserver