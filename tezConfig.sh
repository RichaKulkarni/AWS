#!/bin/bash
sudo aws s3 cp s3://S3_BUCKET_EMR/tez/tez-0.9.0.tar.gz /config
sudo tar -xzf /config/tez-0.9.0.tar.gz -C /config
