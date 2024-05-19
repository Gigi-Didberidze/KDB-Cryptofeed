# KDB-Cryptofeed
Implementation of KDB backend for Cryptofeed

## Requirements
1. Cryptofeed
```bash
pip install cryptofeed
```
2. Pykx
```bash
pip install pykx
```

## Start Tickerplant
```bash
q tick.q sym . -p 5000
```
### Run in background:
```bash
nohup ../q/l64/q tick.q sym . -p 5000 </dev/null >logTickerPlant.txt 2>&1 &
```
### Run with TLS
```bash
su -c 'nohup ../q/l64/q tick.q sym . -u 1 -E 1 -p 5000 </dev/null >logTickerPlant.txt 2>&1 &'
```
## Start RDB

```bash
q tick/r.q localhost:5000 localhost:5002 -p 5001
```
### Run in bacgkround:
```bash
nohup ../q/l64/q tick/r.q localhost:5000 localhost:5002 -p 5001 </dev/null >logRDB.txt 2>&1 &
```
### Run with TLS
```bash
su -c 'nohup ../q/l64/q tick/r.q localhost:5000 localhost:5002 -u 1 -E 1 -p 5001 </dev/null >logRDB.txt 2>&1 &'
```


## Start HDB

 ```bash
 q tick/hdb.q sym -p 5002
 ```
 ### Run in bacgkround:
 ```bash
 nohup ../q/l64/q tick/hdb.q sym -p 5002 </dev/null >logHDB.txt 2>&1 &
 ```
 ### Run with TLS
```bash
su -c 'nohup ../q/l64/q tick/hdb.q sym -u 1 -E 1 -p 5002 </dev/null >logHDB.txt 2>&1 &'
```

## Run the feed

```bash
python feed.py
```
### Run in background:
```bash
nohup python feed.py </dev/null >main_output.txt 2>&1 &
```
 ### Run with TLS
```bash
su -c 'nohup python feed.py </dev/null >main_output.txt 2>&1 &'
```



