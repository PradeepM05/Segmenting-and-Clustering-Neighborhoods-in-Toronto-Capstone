for key, val in prod_dict.items():
    print(f"Finding hierarchy for product: {key} ........ {format(key)}")
    
    # Step 1: Collect the complete hierarchy chain
    hierarchy_chain = []
    current = key
123123123
df = df1.collect()
prod_dict = {}
for row in df:
    prod_dict[row["product_type_id"]] = row["parent_prod_type_id"] or 0

schema = StructType([
    StructField("product_type_id", IntegerType(), True),
    StructField("id_hierarchy", IntegerType(), True),
    StructField("level", IntegerType(), True)
])
df_prod_level = spark.createDataFrame([], schema)
cols = ["product_type_id", "id_hierarchy", "level"]

def find_parent(prod_dict, current):
    return prod_dict.get(current, 0)

for key in prod_dict.keys():
    path = []
    current = key
    while current != 0:
        parent = find_parent(prod_dict, current)
        if parent == 0:
            break
        path.append((parent, key))  # (parent, child)
        current = parent

    # Now assign levels from top-down
    level = 1
    for parent, child in reversed(path):  # root is first, leaf is last
        new_row = spark.createDataFrame([(parent, child, level)], cols)
        df_prod_level = df_prod_level.union(new_row)
        level += 1

# Add identity (level 0) for all products (optional)
df_prod_level_all = df_prod_level.union(
    df1.selectExpr("product_type_id", "product_type_id as id_hierarchy", "0 as level")
)
