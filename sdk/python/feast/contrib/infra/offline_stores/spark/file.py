from typing import Dict, List, Optional, Tuple, Union

from datetime import timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, expr, monotonically_increasing_id, row_number
from pyspark.sql.types import LongType

from feast.data_source import DataSource, FileSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.provider import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    _get_requested_feature_views_to_features_dict,
)


def _read_and_verify_feature_view_df_from_source(
    spark: SparkSession, feature_view: FeatureView, features: List[str],
) -> DataFrame:
    # make sure the format is FileSource
    if not isinstance(feature_view.input, FileSource):
        raise ValueError(f"Only FileSources are currently supported by spark")
    # TODO: currently hardcoded to use parquet
    source_df = spark.read.format("parquet").load(
        feature_view.input.path
    )

    # TODO: add support for created timestamp
    # TODO: add schema verification if needed

    return source_df.select(
        [col(name) for name in features + feature_view.entities]
        + [
            col(feature_view.input.event_timestamp_column).alias(DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL),
        ]
    )


def _filter_feature_table_by_time_range(
    feature_table_df: DataFrame,
    feature_view: FeatureView,
    feature_event_timestamp_column: str,
    entity_df: DataFrame,
    entity_event_timestamp_column: str,
):
    entity_max_timestamp = entity_df.agg(
        {entity_event_timestamp_column: "max"}
    ).collect()[0][0]

    feature_table_timestamp_filter = (
     col(feature_event_timestamp_column) <= entity_max_timestamp
    )

    time_range_filtered_df = feature_table_df.filter(feature_table_timestamp_filter)

    return time_range_filtered_df


class SchemaError(Exception):
    """
    One or more columns in entity or feature table dataframe are either missing
    or have the wrong data types
    """

    pass


def as_of_join(
    entity_df: DataFrame,
    entity_event_timestamp_column: str,
    feature_view_df: DataFrame,
    feature_view: FeatureView,
    features: List[str]
) -> DataFrame:
    """Perform an as of join between entity and feature view, given a maximum age tolerance.
    Join conditions:
    1. Entity primary key(s) value matches.
    2. Feature event timestamp is the closest match possible to the entity event timestamp,
       but must not be more recent than the entity event timestamp.
    3. If more than one feature view rows satisfy condition 1 and 2, feature row with the
       most recent created timestamp will be chosen.
    4. If none of the above conditions are satisfied, the feature rows will have null values.
    Args:
        entity_df (DataFrame): Spark dataframe representing the entities, to be joined with
            the feature tables.
        entity_event_timestamp_column (str): Column name in entity_df which represents
            event timestamp.
        feature_table_df (Dataframe): Spark dataframe representing the feature table.
        feature_table (FeatureView): Feature view specification, which provide information on
            how the join should be performed, such as the entity primary keys and max age.
        featurs: the list of features to extract
    Returns:
        DataFrame: Join result, which contains all the original columns from entity_df, as well
            as all the features specified in feature_view, where the feature columns will
            be prefixed with feature table name.
    """
    entity_with_id = entity_df.withColumn("_row_nr", monotonically_increasing_id())

    feature_event_timestamp_column_with_prefix = (
        f"{feature_view.name}__{DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}"
    )
    # TODO: add creted timestamp column
    # feature_created_timestamp_column_with_prefix = (
    #     f"{feature_view.name}__{CREATED_TIMESTAMP_ALIAS}"
    # )

    projection = [
        col(col_name).alias(f"{feature_view.name}__{col_name}")
        for col_name in feature_view_df.columns
    ]

    aliased_feature_table_df = feature_view_df.select(projection)

    join_cond = (
        entity_with_id[entity_event_timestamp_column]
        >= aliased_feature_table_df[feature_event_timestamp_column_with_prefix]
    )

    for key in feature_view.entities:
        join_cond = join_cond & (
            entity_with_id[key]
            == aliased_feature_table_df[f"{feature_view.name}__{key}"]
        )

    conditional_join = entity_with_id.join(
        aliased_feature_table_df, join_cond, "leftOuter"
    )
    for key in feature_view.entities:
        conditional_join = conditional_join.drop(
            aliased_feature_table_df[f"{feature_view.name}__{key}"]
        )

    window = Window.partitionBy("_row_nr", *feature_view.entities).orderBy(
        col(feature_event_timestamp_column_with_prefix).desc(),
        # col(feature_created_timestamp_column_with_prefix).desc(),
    )
    filter_most_recent_feature_timestamp = conditional_join.withColumn(
        "_rank", row_number().over(window)
    ).filter(col("_rank") == 1)

    return filter_most_recent_feature_timestamp.select(
        entity_df.columns
        + [f"{feature_view.name}__{feature}" for feature in features]
    )


def join_entity_to_feature_tables(
    entity_df: DataFrame,
    entity_event_timestamp_column: str,
    feature_view_dfs: List[DataFrame],
    feature_views: List[FeatureView],
    feature_views_to_features: Dict[str, List[str]]
) -> DataFrame:
    """Perform as of join between entity and multiple feature table.
    Args:
        entity_df (DataFrame): Spark dataframe representing the entities, to be joined with
            the feature tables.
        entity_event_timestamp_column (str): Column name in entity_df which represents
            event timestamp.
        feature_table_dfs (List[Dataframe]): List of Spark dataframes representing the feature tables.
        feature_tables (List[FeatureView]): List of feature table specification. The length and ordering
            of this argument must follow that of feature_table_dfs.
    Returns:
        DataFrame: Join result, which contains all the original columns from entity_df, as well
            as all the features specified in feature_tables, where the feature columns will
            be prefixed with feature table name.
    """
    joined_df = entity_df

    for (feature_view_df, feature_view,) in zip(feature_view_dfs, feature_views):
        joined_df = as_of_join(
            joined_df,
            entity_event_timestamp_column,
            feature_view_df,
            feature_view,
            feature_views_to_features[feature_view]
        )
    return joined_df


class SparkFileRetrievalJob:
    def __init__(self, rJob: RetrievalJob):
        self.config = rJob.config
        self.feature_views = rJob.feature_views
        self.feature_refs = rJob.feature_refs
        self.entity_df = rJob.entity_df
        self.registry = rJob.registry
        self.project = rJob.project
        # get already existing Session
        self.spark = SparkSession.builder.getOrCreate()

    def to_df(self) -> DataFrame:
        """Return dataset as Spark DataFrame synchronously"""
        if not isinstance(self.entity_df, DataFrame):
            raise ValueError(
                f"Please provide an entity_df of type {type(DataFrame)} instead of type {type(self.entity_df)}"
            )
        # load feature dfs and select the columns
        feature_views_to_features = _get_requested_feature_views_to_features_dict(
            self.feature_refs, self.feature_views
        )

        feature_view_dfs = [
            _read_and_verify_feature_view_df_from_source(
                self.spark, feature_view, features
            )
            for feature_view, features in feature_views_to_features.items()
        ]

        # filter by time range
        # TODO: support custom timestamp
        feature_view_dfs = [
            _filter_feature_table_by_time_range(
                feature_view_df,
                feature_view,
                DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
                self.entity_df,
                DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
            )
            for feature_view, feature_view_df in zip(
                self.feature_views, feature_view_dfs
            )
        ]

        # join
        # TODO: support custom setting of the timestamp column
        return join_entity_to_feature_tables(
            self.entity_df,
            DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
            feature_view_dfs,
            self.feature_views,
            feature_views_to_features
        )
