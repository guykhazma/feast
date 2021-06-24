# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import importlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional

import pandas as pd
import pyarrow

from feast import errors
from feast.data_source import DataSource
from feast.feature_view import FeatureView
from feast.registry import Registry
from feast.repo_config import RepoConfig


@dataclass(frozen=True)
class RetrievalJob(ABC):
    """RetrievalJob is used to manage the execution of a historical feature retrieval"""

    config: RepoConfig
    feature_views: List[FeatureView]
    feature_refs: List[str]
    entity_df: Any
    registry: Registry
    project: str

    @abstractmethod
    def to_df(self):
        """Return dataset as Pandas DataFrame synchronously"""
        pass

    def to_engine(self, engine_module) -> Any:
        """Return a dataframe representation compatible with the given engine"""
        # Split engine into module and class names by finding the right-most dot.
        # For example, provider 'foo.bar.MyEngine' will be parsed into 'foo.bar' and 'MyEngine'
        module_name, class_name = engine_module.rsplit(".", 1)

        # Try importing the module that contains the custom engine
        try:
            module = importlib.import_module(module_name)
        except Exception as e:
            # The original exception can be anything - either module not found,
            # or any other kind of error happening during the module import time.
            # So we should include the original error as well in the stack trace.
            raise errors.FeastProviderModuleImportError(engine_module) from e
        # Try getting the engine class definition
        try:
            EngineCls = getattr(module, class_name)
        except AttributeError:
            # This can only be one type of error, when class_name attribute does not exist in the module
            # So we don't have to include the original exception here
            raise errors.FeastProviderClassImportError(
                module_name, class_name
            ) from None

        return EngineCls(self)


class OfflineStore(ABC):
    """
    OfflineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.
    """

    @staticmethod
    @abstractmethod
    def pull_latest_from_table_or_query(
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pyarrow.Table:
        """
        Note that join_key_columns, feature_name_columns, event_timestamp_column, and created_timestamp_column
        have all already been mapped to column names of the source table and those column names are the values passed
        into this function.
        """
        pass

    @staticmethod
    @abstractmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Any,
        registry: Registry,
        project: str,
    ) -> RetrievalJob:
        pass
