for key, val in prod_dict.items():
    print(f"Finding hierarchy for product: {key} ........ {format(key)}")
    
    # Step 1: Collect the complete hierarchy chain
    hierarchy_chain = []
    current = key
    
    while current is not None:
        parent = self.find_parent(prod_dict, current)
        if parent is not None:
            hierarchy_chain.append((parent, current))  # (parent, child) tuple
            current = parent
        else:
            break
    
    # Step 2: Reverse the chain and assign correct levels
    hierarchy_chain.reverse()  # Now most general parent is first
    
    # Step 3: Create DataFrame entries with correct levels
    for i, (parent, child) in enumerate(hierarchy_chain):
        level = i + 1  # Level 1 = most general, Level N = most specific
        print(f"Creating hierarchy: Parent={parent}, Child={child}, Level={level}")
        
        new_col = spark.createDataFrame([(parent, child, level)], cols)
        df_prod_level = df_prod_level.union(new_col)
