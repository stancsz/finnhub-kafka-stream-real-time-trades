# FinnHub Kafka Stream (producer)
This project is a FinnHub Kafka producer in a docker compose image. 


# Prerequisites 
- FinnHub API token
- Docker(with docker compose)

# Setup
Create environment variable `FINNHUB_API_TOKEN`, for example 
```
export FINNHUB_API_TOKEN = '<finnhub-token>'
```

Compose the images with `docker compose up --build`



# Refs
- https://pypi.org/project/websocket_client/
- https://finnhub.io/docs/api/websocket-trades
