# =============================================================================
# FIXED df_prod_hier_final - Replace your current join logic
# =============================================================================

# PROBLEM IN CURRENT CODE:
# You're joining on: prod.prod_typ_cd == hier1.name & (hier1.level == 1)
# This only finds products that directly match level 1, leaving blanks for levels 2-5

# SOLUTION: Join based on the hierarchy relationships, not direct matches

# Step 1: Create a product-to-hierarchy mapping table
df_product_to_hierarchy = df_prod_level_all.select(
    f.col("product_type_id").alias("base_product_type"),
    f.col("id_hierarchy").alias("hierarchy_product_type"), 
    f.col("level")
).distinct()

# Step 2: Get product type names for mapping
df_product_names = df_prod_type.select(
    f.col("product_type_id"),
    f.col("name").alias("product_name"),
    f.col("description").alias("product_desc")
)

# Step 3: Create hierarchy lookup with names
df_hierarchy_with_names = df_product_to_hierarchy.alias("pth")\
    .join(df_product_names.alias("pn"), 
          f.col("pth.hierarchy_product_type") == f.col("pn.product_type_id"), "left")\
    .select(
        f.col("pth.base_product_type"),
        f.col("pth.level"),
        f.col("pn.product_name").alias("Top_Tier_Name"),
        f.col("pn.product_desc").alias("Top_Tier_Desc")
    )

# Step 4: Create separate DataFrames for each level
df_level1 = df_hierarchy_with_names.filter(f.col("level") == 1).alias("l1")
df_level2 = df_hierarchy_with_names.filter(f.col("level") == 2).alias("l2") 
df_level3 = df_hierarchy_with_names.filter(f.col("level") == 3).alias("l3")
df_level4 = df_hierarchy_with_names.filter(f.col("level") == 4).alias("l4")
df_level5 = df_hierarchy_with_names.filter(f.col("level") == 5).alias("l5")
df_level6 = df_hierarchy_with_names.filter(f.col("level") == 6).alias("l6")

# Step 5: CORRECTED df_prod_hier_final - Join on product_type_id, not prod_typ_cd
df_prod_hier_final = df_prod_cd.alias("prod")\
    .join(df_level1, f.col("prod.prod_typ_cd") == f.col("l1.base_product_type"), "left")\
    .join(df_level2, f.col("prod.prod_typ_cd") == f.col("l2.base_product_type"), "left")\
    .join(df_level3, f.col("prod.prod_typ_cd") == f.col("l3.base_product_type"), "left")\
    .join(df_level4, f.col("prod.prod_typ_cd") == f.col("l4.base_product_type"), "left")\
    .join(df_level5, f.col("prod.prod_typ_cd") == f.col("l5.base_product_type"), "left")\
    .join(df_level6, f.col("prod.prod_typ_cd") == f.col("l6.base_product_type"), "left")\
    .withColumn("prd_cd", f.trim(f.col("prod.prod_cd")))\
    .withColumn("prd_nm", f.trim(f.col("prod.prod_nm")))\
    .withColumn("prd_shrt_nm", f.trim(f.col("prod.prod_shrt_nm")))\
    .withColumn("prd_long_nm", f.trim(f.col("prod.prod_long_nm")))\
    .withColumn("prd_efct_dt", f.col("prod.prod_efct_dt"))\
    .withColumn("prd_exp_dt", f.col("prod.prod_exp_dt"))\
    .withColumn("prd_last_update_dt", f.col("prod.prod_last_update_dt"))\
    .withColumn("prd_struct_typ", f.col("prod.prod_struct_typ"))\
    .withColumn("prd_struct_de_tx", f.col("prod.prod_struct_de_tx"))\
    .withColumn("prd_status_typ", f.col("prod.prod_status_typ"))\
    .withColumn("prd_status_typ_de_tx", f.col("prod.prod_status_typ_de_tx"))\
    .withColumn("prd_status_typ_update_dt", f.col("prod.prod_status_typ_update_dt"))\
    .withColumn("prd_status_reason_typ", f.col("prod.prod_status_reason_typ"))\
    .withColumn("prd_status_reason_de_tx", f.col("prod.prod_status_reason_de_tx"))\
    .withColumn("prd_typ_cd", f.col("prod.prod_typ_cd"))\
    .withColumn("prd_typ_nm", f.col("prod.prod_typ_nm"))\
    .withColumn("prd_own_cc_cd", f.col("prod.prod_own_cc_cd"))\
    .withColumn("prd_versn_no", f.col("prod.prod_versn_no"))\
    .withColumn("prd_arng_prd_cd", f.col("prod.prod_arng_prd_cd"))\
    .withColumn("prd_incm_tax_deductable_flg", f.col("prod.incm_tax_deductable_flg"))\
    .withColumn("prd_cst_acc_rqr_flg", f.col("prod.cst_acc_rqr_flg"))\
    .withColumn("prd_hive_sor_dt", f.col("prod.hive_sor_dt"))\
    .withColumn("prd_hive_sys_dt", f.col("prod.hive_sys_dt"))\
    .withColumn("prdtyp_product1", f.trim(f.col("l1.Top_Tier_Desc")))\
    .withColumn("prdtyp_product1cd", f.trim(f.col("l1.Top_Tier_Name")))\
    .withColumn("prdtyp_product2", f.trim(f.col("l2.Top_Tier_Desc")))\
    .withColumn("prdtyp_product2cd", f.trim(f.col("l2.Top_Tier_Name")))\
    .withColumn("prdtyp_product3", f.trim(f.col("l3.Top_Tier_Desc")))\
    .withColumn("prdtyp_product3cd", f.trim(f.col("l3.Top_Tier_Name")))\
    .withColumn("prdtyp_product4", f.trim(f.col("l4.Top_Tier_Desc")))\
    .withColumn("prdtyp_product4cd", f.trim(f.col("l4.Top_Tier_Name")))\
    .withColumn("prdtyp_product5", f.trim(f.col("l5.Top_Tier_Desc")))\
    .withColumn("prdtyp_product5cd", f.trim(f.col("l5.Top_Tier_Name")))\
    .withColumn("prdtyp_product6", f.trim(f.col("l6.Top_Tier_Desc")))\
    .withColumn("prdtyp_product6cd", f.trim(f.col("l6.Top_Tier_Name")))\
    .withColumn("ecr_etl_load_ts", f.current_timestamp())\
    .select("prd_cd", "prd_nm", "prd_shrt_nm", "prd_long_nm", "prd_efct_dt", "prd_exp_dt", "prd_last_update_dt", "prd_struct_typ", "prd_struct_de_tx",
           "prd_status_typ", "prd_status_typ_de_tx", "prd_status_typ_update_dt", "prd_status_reason_typ", "prd_status_reason_de_tx", "prd_typ_cd", "prd_typ_nm",
           "prd_own_cc_cd", "prd_versn_no", "prd_arng_prd_cd", "prd_incm_tax_deductable_flg", "prd_cst_acc_rqr_flg", "prd_hive_sor_dt", "prd_hive_sys_dt",
           "prdtyp_product1", "prdtyp_product1cd", "prdtyp_product2", "prdtyp_product2cd", "prdtyp_product3", "prdtyp_product3cd", "prdtyp_product4", "prdtyp_product4cd",
           "prdtyp_product5", "prdtyp_product5cd", "prdtyp_product6", "prdtyp_product6cd", "ecr_etl_load_ts")

# =============================================================================
# ALTERNATIVE SIMPLER APPROACH (if above is too complex)
# =============================================================================
