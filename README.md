# MR-Top

## How-to

### Run the hadoop via docker

```
docker run -d --name hadoop-test -P sequenceiq/hadoop-docker:latest
```

### Change the current user

Change to user `hadoop` by running `su - hadoop`.

### Prepare data

Assuming you have a data file which name is `data`, run commands below to move it to HDFS at file `/kaola/order/input`.

```bash
bin/hadoop fs -mkdir -p /kaola/order/
bin/hadoop fs -moveFromLocal data /kaola/order/input
```

Each line of the data file should look like below.

```
0|256988385197167935|100001|1|3|159.52|149.52|9718470045892|2018-11-09 10:54:52|甘肃|兰州|城关区盘旋路100号|12,99,32
```

### Clone the repository

`git clone https://github.com/cloudtogo/mr_top.git`

### Run the task to figure out the top 3 products each day

`cd mr_top && ./run.sh`

## Modify code to achieve more

Modifying `src/kaola/Config` could change the behavior of the program.

`kaola.Config.topN` indicates how many items of each day will show in the result.

`kaola.Config.keyAttributeIdx` defines the index of the column you would like to sort in the data file.It should start from 0.
