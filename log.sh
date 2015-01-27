CMD=$1
NODE=$2
LOG=$3
ssh $NODE $CMD /home/hadoop/rcor/CloudOS/data/$NODE/$LOG
