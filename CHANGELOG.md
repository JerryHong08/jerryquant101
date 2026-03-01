## 0.2.0 (2026-03-01)

### Feat

- **src/live_monitor/mmm/core/data**: News Fetcher: momo web, fmp, benzinga. Add Logger
- **src/live_monitor/mmm/core/factor_engine**: Factor Manager prototype done
- **live_monitor/mmm/factor_engine**: refactor and build up a robust collector and backend to frontend, next working on fatcor engine building
- **live_monitor/mmm/data_collector&backend-api**: MMM Redis to Redis Stream. Backend decoupled to Redis_client.py as data receiver

### Fix

- **src/backtest**: git commit -m "trades_analyzer refactor and delisted status bug fixed"
- **src-scripts**: versatile -> indcies_update, changed dir, fixed proxy problem
- **live_monitor/mmm**: Replay mode to Redis Stream

### Refactor

- **src**: prune & clean up
- gridtrader. changed zset to redis_stream:market_snapshot_processed. reload all fixed.
- gridtrader backend refactor backup
