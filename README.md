# deche

A small caching decorator

### Usage
Create a cache instance, and wrap any required functions in the instance decorator.

```python
import requests
from deche import Cache

# Create a cache instance
cache = Cache(prefix="/home/user/data/cache")

@cache
def get(url):
    return requests.get(url).content
```

