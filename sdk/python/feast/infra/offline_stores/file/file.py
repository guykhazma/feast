from datetime import datetime
from typing import Callable, List, Optional, Union

import pandas as pd
import pyarrow
import pytz

from feast.data_source import DataSource, FileSource
from feast.errors import FeastJoinKeysDuringMaterialization
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.offline_stores.file.pandas.pandas_retrival_job import PandasFileRetrievalJob
from feast.infra.offline_stores.file.spark.spark_retrival_job import SparkFileRetrievalJob

from feast.infra.provider import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    _get_requested_feature_views_to_features_dict,
    _run_field_mapping,
)
from feast.registry import Registry
from feast.repo_config import RepoConfig

class FileOfflineStore(OfflineStore):
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        engine: str = 'pandas'
    ) -> RetrievalJob:
        if engine == 'pandas':
            return PandasFileRetrievalJob(config,
                    feature_views,
                    feature_refs,
                    entity_df,
                    registry,
                    project)
        elif engine == 'spark':
            return SparkFileRetrievalJob(config,
                    feature_views,
                    feature_refs,
                    entity_df,
                    registry,
                    project)
        raise ValueError(f"Unsupported offline store engine '{engine}'")

    @staticmethod
    def pull_latest_from_table_or_query(
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pyarrow.Table:
        assert isinstance(data_source, FileSource)

        source_df = pd.read_parquet(data_source.path)
        # Make sure all timestamp fields are tz-aware. We default tz-naive fields to UTC
        source_df[event_timestamp_column] = source_df[event_timestamp_column].apply(
            lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc)
        )
        if created_timestamp_column:
            source_df[created_timestamp_column] = source_df[
                created_timestamp_column
            ].apply(lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc))

        source_columns = set(source_df.columns)
        if not set(join_key_columns).issubset(source_columns):
            raise FeastJoinKeysDuringMaterialization(
                data_source.path, set(join_key_columns), source_columns
            )

        ts_columns = (
            [event_timestamp_column, created_timestamp_column]
            if created_timestamp_column
            else [event_timestamp_column]
        )

        source_df.sort_values(by=ts_columns, inplace=True)

        filtered_df = source_df[
            (source_df[event_timestamp_column] >= start_date)
            & (source_df[event_timestamp_column] < end_date)
        ]
        last_values_df = filtered_df.drop_duplicates(
            join_key_columns, keep="last", ignore_index=True
        )

        columns_to_extract = set(join_key_columns + feature_name_columns + ts_columns)
        table = pyarrow.Table.from_pandas(last_values_df[columns_to_extract])

        return table
