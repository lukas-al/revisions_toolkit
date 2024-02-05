from kedro.pipeline import node
from kedro.pipeline.modular_pipeline import pipeline

from .nodes import load_quarterly_data, clean_quarterly_data, save_data, transform_and_combine


def create_pipeline(**kwargs):
    return pipeline(
        [
            node(
                func=load_quarterly_data,
                inputs=["headline_qgdp_vintages"], # Input
                outputs="loaded_QGDP_df",
                name="Load_QGDP_data",
            ),
            node(
                func=clean_quarterly_data,
                inputs=["loaded_QGDP_df"],
                outputs="clean_QGDP_df",
                name="Clean_QGDP_data",
            ),
            node(
                func=transform_and_combine,
                inputs=["clean_QGDP_df"],
                outputs="transformed_QGDP_dict",
                name="Transform_QGDP_data",
            ),
            node(
                func=save_data,
                inputs=["transformed_QGDP_dict", "params:quarterly_gdp_clean_filename"],
                outputs=None,
                name="Save_QGDP_data",
            ),
        ]
    )