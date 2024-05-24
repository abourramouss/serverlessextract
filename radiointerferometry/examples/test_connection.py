from lithops import Storage


storage = Storage()

res = storage.list_keys('ayman-extract', prefix='partitions/partition_1/')

print(res)