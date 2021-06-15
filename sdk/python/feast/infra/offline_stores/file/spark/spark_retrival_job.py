from typing import List, Union

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, expr, monotonically_increasing_id, row_number
from pyspark.sql.types import LongType

import pandas as pd
import pyarrow
import pytz

from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.provider import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    _get_requested_feature_views_to_features_dict,
    _run_field_mapping,
)
from feast.registry import Registry
from feast.repo_config import RepoConfig

# could be in a different package
class SparkFileRetrievalJob(RetrievalJob):
    def __init__(self,
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[DataFrame, str],
        registry: Registry,
        project: str,
        ):
        # currently spark dataframe
        if not isinstance(entity_df, DataFrame):
            raise ValueError(
                f"Please provide an entity_df of type {type(DataFrame)} instead of type {type(entity_df)}"
            )
        
        # TODO: logic

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():
            pass

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluate_historical_retrieval

    def to_df(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function()
        return df