import json

json_data = '''
{
    "links": [
        {
            "count": 0,
            "category": {
                "id": 10115435,
                "category_id": 1,
                "title": "Test 13/05"
            }
        },
        {
            "count": 0,
            "category": {
                "id": 10440337,
                "category_id": 1,
                "title": "Категория услуг"
            }
        }
    ]
}
'''

data = json.loads(json_data)

services = {
    "category": [],
    "service": []
}

for link in data["links"]:
    if "service" in link:
        if "id" in link["service"]:
            services["service"].append(link["service"]["id"])
    if "category" in link:
        if "id" in link["category"]:
            services["category"].append(link["category"]["id"])

print(services)
