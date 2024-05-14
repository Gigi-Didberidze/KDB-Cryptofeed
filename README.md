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

## Start RDB

```bash
q tick/r.q localhost:5000 localhost:5002 -p 5001
```

## Start HDB

 ```bash
 q tick/hdb.q sym -p 5002
 ```

## Run the feed

```bash
python feed.py
```



