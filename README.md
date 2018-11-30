# MR-Top

## How-to

### Create the dir and clone the repository

```bash
cd /root
mkdir input
docker clone https://github.com/gobomb/mr_top2.git
```

### Prepare data

```
cd /root/mr_top2
./getdata.sh
```

Each line of the data file should look like below.

```
0|256988385197167935|100001|1|3|159.52|149.52|9718470045892|2018-11-09 10:54:52|甘肃|兰州|城关区盘旋路100号|12,99,32
```


### Run the task to figure out the top 3 products each day

`cd mr_top2 && ./run.sh`

## Modify code to achieve more

Modifying `src/kaola/Config` could change the behavior of the program.

`kaola.Config.topN` indicates how many items of each day will show in the result.

`kaola.Config.keyAttributeIdx` defines the index of the column you would like to sort in the data file.It should start from 0.
