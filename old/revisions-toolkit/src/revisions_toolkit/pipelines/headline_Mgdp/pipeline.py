"""A pipeline organises the dependencies and execution order of your collection of nodes, 
and connects inputs and outputs while keeping your code modular. 
The pipeline resolves dependencies to determine the node execution order, 
and does not necessarily run the nodes in the order in which they are passed in.
"""

from kedro.pipeline import node
from kedro.pipeline.modular_pipeline import pipeline

# from .nodes import 
from .nodes import clean_monthly_data
from revisions_toolkit.pipelines.headline_Qgdp.nodes import transform_and_combine, save_data, load_data

def create_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=load_data,
                inputs=[
                    "headline_mgdp_vintages",
                    "params:headline_mgdp_filename_list"
                    ],
                outputs="loaded_MGDP_df",
                name="Load_MGDP_data",
            ),
            node(
                func=clean_monthly_data,
                inputs=["loaded_MGDP_df"],
                outputs="clean_MGDP_df",
                name="Clean_MGDP_data",
            ),
            node(
                func=transform_and_combine,
                inputs=["clean_MGDP_df"],
                outputs="transformed_MGDP_dict",
                name="Transform_MGDP_data",
            ),
            node(
                func=save_data,
                inputs=[
                    "transformed_MGDP_dict", 
                    "params:headline_mgdp_filename_list",
                    "params:headline_mgdp_writepath"
                ],
                outputs=None,
                name="Save_MGDP_data",
            ),
        ]
    )