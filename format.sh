ssh master /home/hadoop/rcor/CloudOS/data/master/rm-slave.sh
ssh master rm -r /home/hadoop/rcor/CloudOS/data/master/*

ssh slave1 /home/hadoop/rcor/CloudOS/data/slave1/rm-slave.sh
ssh slave1 rm -r /home/hadoop/rcor/CloudOS/data/slave1/*

ssh slave2 /home/hadoop/rcor/CloudOS/data/slave2/rm-slave.sh
ssh slave2 rm -r /home/hadoop/rcor/CloudOS/data/slave2/*

ssh slave3 /home/hadoop/rcor/CloudOS/data/slave3/rm-slave.sh
ssh slave3 rm -r /home/hadoop/rcor/CloudOS/data/slave3/*

ssh slave4 /home/hadoop/rcor/CloudOS/data/slave4/rm-slave.sh
ssh slave4 rm -r /home/hadoop/rcor/CloudOS/data/slave4/*
