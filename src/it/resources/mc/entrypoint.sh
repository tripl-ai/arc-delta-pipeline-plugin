#!/bin/sh

sleep 3
mc config host add minio http://minio:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}
mc admin policy add minio delta /delta.json
mc admin user add minio deltaUser deltaSecret
mc admin policy set minio delta user=deltaUser