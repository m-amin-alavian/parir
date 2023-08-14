import hbsir
import yaml
import pandas as pd
from hbsir.metadata_reader import metadata, open_yaml
from pdb import set_trace

metadata.reload_file("tables")
metadata.reload_file("schema")
metadata.reload_file("commodities")


# durable_groups = tbl.view["eyn_durable4"]
# durable_groups.Durable_Group.value_counts()


def get_durable_df():
    durables = open_yaml("metadata/durable.yaml", location="root")
    return pd.DataFrame(durables).explode("Code")


durable_tbl = hbsir.load_table("durable", [1397], recreate=True, on_missing="create")
durable_info = get_durable_df()
durable_ownership = (
    pd.merge(durable_tbl[["ID", "Code"]], durable_info, on="Code", how="left")
    .assign(Durable_Good=lambda x: x.Name.astype("category"))
    .groupby(["ID", "Durable_Good"], dropna=False, as_index=False)
    .size()
    .assign(Owns=lambda x: x["size"] > 0)
    .drop(columns="size")
)
durable_ownership
