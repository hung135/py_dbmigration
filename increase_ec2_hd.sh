#dfhttps://docs.aws.amazon.com/AWSEC2/latest/UserGuide/recognize-expanded-volume-linux.html
instanceid=$(curl http://169.254.169.254/latest/meta-data/instance-id/)
volumeid=$(aws ec2 describe-instances --instance-id $instanceid |grep VolumeId | sed  's/^.*Vol.*\(vol.*\)".*/\1/')
aws ec2 describe-volumes --volume-ids $volumeid
 
aws ec2 modify-volume --size 50 --volume-id $volumeid
#lsblk
#df -lh
sudo growpart /dev/xvda1 1