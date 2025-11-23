
# MMM

---

market-monitor-mover/
├── core/
│   ├── collector/                # Data Fetcher (REST / WebSocket / replayer)
│   │   ├── fetcher.py
│   │   ├── websocket.py
│   │   └── replay.py
│   ├── storage/                  # Data Storage (Redis/Parquet/Cache/...)
│   │   ├── redis_client.py
│   │   └── snapshot_store.py
│   ├── data/
│   │   ├── schema.py             # DataFrame schema & validation
│   │   ├── transforms.py         # data ETL clean process
│   │   └── aggregator.py         # trade -> snapshot aggregator
│   ├── engine/
│   │   ├── factor_base.py        # Abstract Factor Base
│   │   ├── factor_loader.py      # factor loader
│   │   └── factors/
│   │       ├── fib_pattern.py
│   │       ├── news_sentiment.py
│   │       ├── technical_vwap.py
│   │       └── cross_hotness.py
│   ├── analyzer/
│   │   ├── ranker.py             # ranker
│   │   ├── highlighter.py        # highlighter
│   └── api/
│       ├── web_server.py         # backend
│       └── serializers.py        # to Frontend dict/json
│
├── frontend/                     # frontend
│   ├── static/
│   └── templates/
├── scripts/                      # Start scripts
│   ├── start.py
│   └── analyzer.py
└── tests/
    ├── test_factors.py
    └── test_transforms.py
