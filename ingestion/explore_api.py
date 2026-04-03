import requests
import json

BASE_URL = "https://dummyjson.com"

# Added ?limit to keep the output clean and manageable
endpoints = {
    "products": "/products?limit=15",
    "carts": "/carts?limit=8",
    "users": "/users?limit=10",
}


for name, path in endpoints.items():
    url = BASE_URL + path
    print(f"\n{'='*75}")
    print(f"Endpoint: {name} , {url}")
    
    try:
        response = requests.get(url, timeout=15)
        print(f"Status code: {response.status_code}")
        print(f"Content-Type: {response.headers.get('Content-Type', 'Not set')}")
        
        if response.status_code != 200:
            print(f"Failed with status {response.status_code}")
            continue
        
        data = response.json()
        
        # DummyJSON usually wraps the list: e.g., {"products": [...], "total": ..., "skip": ..., "limit": ...}
        if isinstance(data, dict):
            # Try to find the main array (products, carts, or users)
            if name in data:
                records = data[name]
            elif "data" in data:          # fallback
                records = data["data"]
            else:
                # If no obvious key, take the first list value we find
                records = next((v for v in data.values() if isinstance(v, list)), data)
        else:
            records = data
        
        count = len(records) if isinstance(records, list) else "N/A"
        print(f"Success , Record count: {count}")
        
        if isinstance(records, list) and len(records) > 0:
            print(f"Sample record keys: {list(records[0].keys())}")
            print(f"Sample record:\n{json.dumps(records[0], indent=2)}")
        else:
            print(f"Preview:\n{json.dumps(records, indent=2)[:700]}...")
            
    except requests.exceptions.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        print(f"Raw response start: {response.text[:400]}")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
