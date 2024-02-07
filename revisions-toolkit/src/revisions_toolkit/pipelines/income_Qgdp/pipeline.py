"""A pipeline organises the dependencies and execution order of your collection of nodes, 
and connects inputs and outputs while keeping your code modular. 
The pipeline resolves dependencies to determine the node execution order, 
and does not necessarily run the nodes in the order in which they are passed in.
"""


from kedro.pipeline import node
from kedro.pipeline.modular_pipeline import pipeline


from revisions_toolkit.pipelines.headline_Qgdp.nodes import (
    transform_and_combine, 
    save_data, 
    load_data, 
    clean_quarterly_data
)

def create_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=load_data,
                inputs=[
                    "income_qgdp_vintages",
                    "params:income_qgdp_filename_list"
                    ],
                outputs="loaded_income_df_list",
                name="Load_income_data",
            ),
            node(
                func=clean_quarterly_data,
                inputs=["loaded_income_df_list"],
                outputs="clean_income_df_list",
                name="Clean_income_data",
            ),
            node(
                func=transform_and_combine,
                inputs=["clean_income_df_list"],
                outputs="transformed_income_df_list",
                name="Transform_income_data",
            ),
            node(
                func=save_data,
                inputs=[
                    "transformed_income_df_list", 
                    "params:income_qgdp_filename_list",
                    "params:income_writepath"
                ],
                outputs=None,
                name="Save_income_data",
            ),
        ]
    )