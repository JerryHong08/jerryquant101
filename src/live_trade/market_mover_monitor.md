# Market Mover Monitor

Currently this is a small part of my quantitative trading project running in my windows wsl2. And now it only has the basic polygon.io
whole market snapshot fetch, filter, rank and save into file features.

I want to make this file into a web monitor with a web front-end so that I can monitor the market mover tickers.But now I want to only make the first stage, showing the results in the terminal is ok.
So, I want,
when it starts, it should have the function that fecth and rank the top N in a regular updating time routine. And it print out a result table, with more informative columns I want to add. Currently it will only output once and end, but I want it to keep update untill I ctrl+c to stop it. 
 more info below:

the Current Stage is what now it is, and the Stage 1 is what I want you to build.

## Current Stage

1. polygon.io snapshot restful api fetch and rank top20.
2. joined on the 'only_common_stocks' method tickers to make sure only cs stocks.
3. save into the cache_dir.

current output table shape and schema:

``` bash
shape: (20, 5)

Schema([('ticker', String), ('prev_close', Float64), ('close', Float64), ('percent_change', Float64), ('open', Float64)])
```

## Stage 1

1. I want to  record each tickers' fecth and last update time so that I can tell if this is a new mover or it has been in the top list for a while. That can help me catch a new mover a great buy choice before anyone else. So, add the ('since exploded', Float64), count in minute.
2. In the future, I want to add more informative columns like if it has news, float shares, cumulative volumes since exploded and so on. Now you can add whatever you think it's informative for me to catch a great opportunity to buy a pre-market mover ticker.
