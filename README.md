
# PROBLEM IN YOUR CURRENT CODE:
# You're joining hierarchy levels incorrectly. The levels are now correct (1=general, 6=specific)
# but the join logic needs to be fixed to properly map to prdtyp_product1 through prdtyp_product6

# REPLACE YOUR CURRENT MAPPING CODE WITH THIS CORRECTED VERSION:
# =============================================================================

# Step 1: Create the base DataFrame with product codes
df_prod_cd = spark.table(product_tbl).alias("prod")

# Step 2: Create hierarchy level DataFrames (assuming df_prod_level is correctly built)
df_level1 = df_prod_level.filter(f.col("level") == 1).alias("h1")  # Most general
df_level2 = df_prod_level.filter(f.col("level") == 2).alias("h2")
df_level3 = df_prod_level.filter(f.col("level") == 3).alias("h3")
df_level4 = df_prod_level.filter(f.col("level") == 4).alias("h4")
df_level5 = df_prod_level.filter(f.col("level") == 5).alias("h5")
df_level6 = df_prod_level.filter(f.col("level") == 6).alias("h6")  # Most specific

# Step 3: Build the hierarchy chain by joining from most specific to most general
# Start with the product and work backwards through the hierarchy

# CORRECTED APPROACH - Join in reverse order to build the chain:
df_prod_hier_final = df_prod_cd\
    .join(df_level6, f.col("prod.product_type_id") == f.col("h6.child_id"), "left")\
    .withColumn("prdtyp_product6", f.col("h6.parent_id"))\
    .join(df_level5, f.col("h6.parent_id") == f.col("h5.child_id"), "left")\
    .withColumn("prdtyp_product5", f.col("h5.parent_id"))\
    .join(df_level4, f.col("h5.parent_id") == f.col("h4.child_id"), "left")\
    .withColumn("prdtyp_product4", f.col("h4.parent_id"))\
    .join(df_level3, f.col("h4.parent_id") == f.col("h3.child_id"), "left")\
    .withColumn("prdtyp_product3", f.col("h3.parent_id"))\
    .join(df_level2, f.col("h3.parent_id") == f.col("h2.child_id"), "left")\
    .withColumn("prdtyp_product2", f.col("h2.parent_id"))\
    .join(df_level1, f.col("h2.parent_id") == f.col("h1.child_id"), "left")\
    .withColumn("prdtyp_product1", f.col("h1.parent_id"))
