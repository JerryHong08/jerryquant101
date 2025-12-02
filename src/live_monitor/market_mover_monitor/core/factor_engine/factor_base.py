# Abstract Factor Base
from abc import ABC, abstractmethod


class FactorBase(ABC):
    @abstractmethod
    def compute(self, snaphsot):
        """return factor score"""
        pass
