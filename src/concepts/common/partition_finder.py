from typing import Any, List, Sequence, Generic, TypeVar


from abc import ABC, abstractmethod
from dataclasses import dataclass
from dagster import AssetsDefinition, AssetMaterialization


T = TypeVar("T")


class PartitionFinder(Generic[T]):
    @abstractmethod
    def find_partitions(self, input_obj: T) -> List[str]:
        """
        finds partitions from the input
        Args:
            input_obj:

        Returns:

        """
        pass


@dataclass
class AssetMetaPartitionFinder(PartitionFinder[AssetMaterialization]):
    meta_field_name: str

    def find_partitions(self, input_obj: AssetMaterialization) -> List[str]:
        """
        Finds Partitions from Asset's Metadata
        Args:
            input_obj:
        """
        metadata_value = input_obj.metadata.get(self.meta_field_name)
        return metadata_value.value if metadata_value else []


@dataclass
class DbTablePartitionFinder(PartitionFinder[str]):
    def find_partitions(self, input_obj: str) -> List[str]:
        """
        input_obj is the sql string used to find the partition list. It should return an array of strings
        Args:
            input_obj:
        """
        return []
