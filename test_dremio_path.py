from connection.dremio_api import DremioAPI

api = DremioAPI()
api.login()
catalog = api.get_catalog()
print("Catalog items:")
for item in catalog.get('data', []):
    print(item['name'])

def print_children(path):
    print(f"\nChildren of {'.'.join(path)}:")
    res = api.get_catalog_by_path(path)
    if res and 'children' in res:
        for child in res['children']:
            print(child['path'])

print_children(["lakehouse"])
print_children(["lakehouse", "raw"])
print_children(["lakehouse", "raw", "nba"])
