from abc import ABC, abstractmethod
from typing import Iterable

from cdc.DataRow import DataRow


class BaseSourceAdapter(ABC):
    @abstractmethod
    def get_target_data(self) -> Iterable[DataRow]:
        raise NotImplementedError()
