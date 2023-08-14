import hbsir
import pandas as pd
import numpy as np
from hbsir.metadata_reader import metadata, open_yaml

metadata.reload_file("tables")
metadata.reload_file("schema")
metadata.reload_file("commodities")


# durable_groups = tbl.view["eyn_durable4"]
# durable_groups.Durable_Group.value_counts()


def get_durable_df():
    durables = open_yaml("metadata/durable.yaml", location="root")
    return pd.DataFrame(durables).explode("Code")


durable_tbl = hbsir.load_table("durable", [1397], recreate=True, on_missing="create")
durable_g2 = durable_tbl.view["eyn_durable4"].query("Durable_Group == 'G2'")
# TODO Need to implement with deciles as well
# TODO Weights seem to be different from Eynian...
durable_value_average = (
    durable_g2.query("Expenditure > 0")
    .groupby("Code", as_index=False)
    .apply(
        lambda x: pd.Series(
            {
                "Count": len(x),
                "Value_Average": np.average(x.Expenditure * 12, weights=x.Weight),
            }
        )
    )
)

durable_info = (
    pd.merge(get_durable_df(), durable_value_average, on="Code", how="left")
    .assign(Value_Depreciated=lambda x: x.Value_Average * x.Depreciation / 100)
    .rename(columns={"Name": "Durable_Good"})
)

durable_ownership = (
    pd.merge(durable_tbl[["ID", "Code"]], durable_info, on="Code", how="left")
    .assign(Durable_Good=lambda x: x["Durable_Good"].astype("category"))
    .groupby(["ID", "Durable_Good"], dropna=False, as_index=False)
    .size()
    .assign(Owns=lambda x: x["size"] > 0)
    .drop(columns="size")
)
durable_ownership = pd.merge(
    durable_ownership,
    durable_info[["Durable_Good", "Value_Average", "Value_Depreciated"]],
    on="Durable_Good",
    how="left",
)
durable_ownership.groupby("ID").apply(
    lambda x: pd.Series(
        {
            "Value_Average": np.sum(x.Value_Average * x.Owns),
            "Value_Depreciated": np.sum(x.Value_Depreciated * x.Owns) / 12,
        }
    )
)
