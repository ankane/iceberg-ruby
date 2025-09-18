import pyarrow as pa
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    'main',
    type='rest',
    uri='http://localhost:8181'
)

a = pa.array([1, 2], type=pa.int64())
b = pa.array([10, 20], type=pa.int32())
df = pa.table([a, b], names=['a', 'b'])

table = catalog.create_table('iceberg_ruby_test.events', schema=df.schema)
table.append(df)

with table.update_schema() as update:
    update.rename_column('a', 'c')
    update.delete_column('b')

c = pa.array([3], type=pa.int64())
df2 = pa.table([c], names=['c'])

table.append(df2)
