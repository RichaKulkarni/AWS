#!/bin/bash
sudo set -e
sudo wget -S -T 10 -t 5 http://elasticmapreduce.s3.amazonaws.com/bootstrap-actions/file.tar.gz
sudo mkdir -p /config
sudo tar -xzf file.tar.gz -C /config
