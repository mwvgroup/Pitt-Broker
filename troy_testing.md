```python
ipython

from kafka import KafkaConsumer
from mock_stream import prime_alerts
from mock_stream import download_data

download_data()
consumer = KafkaConsumer('ztf-stream',bootstrap_servers=['localhost:9092'])
prime_alerts(max_alerts=25)
for alert in consumer:
    print(alert)
```
