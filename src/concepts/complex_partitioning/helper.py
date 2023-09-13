import polars as pl

def save_all_chunks(chunked_dfs):
    for k, v in chunked_dfs.items():
        save_df(k, v)


def make_dataframe_chunks(config, inventory_df):
    return inventory_df.select(pl.col("*"),
                               pl.col("Size").alias("CumSize").cast(pl.Int64).cumsum()
                               ).with_columns(pl.col("*"),
                                              pl.col("CumSize").alias("batch_num") // config.max_batch_size
                                              ).partition_by("batch_num", as_dict=True)


def save_df(k, v):
    if k == 0:
        print(f"Saving Data Frame ... \n {v}")
