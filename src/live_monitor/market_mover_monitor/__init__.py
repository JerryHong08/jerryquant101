# Market Mover Monitor Package
# API tick
#    ↓ (collector)
# raw data (untrusted, inconsistent)
#    ↓ (storage)
# serialized, versioned, timestamped, reproducible
#    ↓ (data)
# cleaned, validated, aggregated snapshot → Factor Engine
