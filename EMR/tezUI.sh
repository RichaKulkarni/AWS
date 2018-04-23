#!/bin/bash
sudo rm -rf /mnt/var/lib/tomcat8/webapps/tez-ui/
sudo aws s3 cp s3://S3_BUCKET_EMR/tez-ui.tar.gz /mnt/var/lib/tomcat8/webapps/tez-ui/
sudo tar -xzf /mnt/var/lib/tomcat8/webapps/tez-ui/tez-ui.tar.gz -C /mnt/var/lib/tomcat8/webapps/tez-ui/
export IP=`hostname -i`
sudo -E bash -c 'echo $IP'
sudo sed -i "s/localhost/$IP/g" /mnt/var/lib/tomcat8/webapps/tez-ui/config/configs.env
sudo sed -i "s/\/\/timeline:/timeline:/g" /mnt/var/lib/tomcat8/webapps/tez-ui/config/configs.env
sudo sed -i "s/\/\/rm:/rm:/g" /mnt/var/lib/tomcat8/webapps/tez-ui/config/configs.env
sudo sed -i "s/<value>http:\/\/ip-.*/<value>http:\/\/$IP:8080\/tez-ui\/<\/value>/g" /etc/tez/conf/tez-site.xml
