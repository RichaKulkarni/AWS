#!/bin/bash
export cluster_name=$1
export core_count=$2

echo $cluster_name
echo $core_count

#print master IP
masterIP=`aws ec2 describe-instances --filters "Name=tag:emr_cluster_name,Values=$cluster_name" "Name=tag:aws:elasticmapreduce:instance-group-role,Values=MASTER" --query 'Reservations[*].Instances[*].NetworkInterfaces[*].PrivateIpAddresses[*].PrivateIpAddress[]' --region us-east-1 --output text`
echo $masterIP
#print RM URL
fullURL="http://$masterIP:8088/"
echo $fullURL

#describe core node IPs
coreIP=`aws ec2 describe-instances --filters "Name=tag:emr_cluster_name,Values=$cluster_name" "Name=tag:aws:elasticmapreduce:instance-group-role,Values=CORE" --query 'Reservations[*].Instances[*].NetworkInterfaces[*].PrivateIpAddresses[*].PrivateIpAddress[]' --region us-east-1 --output text`
echo "CORE INSTANCES"
echo $coreIP

#create empty array and store core node IPs in there, then iterate through them to attach Node labels
arr=()
for i in $(seq 1 $core_count); do
   echo "i = "
   echo $i
   echo "f = "
   f="-f$i"
   echo $f
   arr[$i]=$(echo $coreIP | cut $f -d ' ') 
   currCORE=`echo ${arr[$i]}`
   echo $currCORE
   #change ip from . to - for node label use
   nodeLabelCORE=`echo "${currCORE//./-}"`
   echo $nodeLabelCORE
   #attach node label to core nodes
   fullNodeLabelCommand="sudo yarn rmadmin -replaceLabelsOnNode ip-$nodeLabelCORE=CORE"
   echo $fullNodeLabelCommand
   #ssh into each core node and run node attach command on them
   sudo ssh -o StrictHostKeyChecking=no -i /root/.ssh/yours.pem ec2-user@$currCORE "$fullNodeLabelCommand"  
done
