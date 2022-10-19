from typing import Mapping
from api import m_f
from kgdata.wikidata.models.wdentity import WDEntity


class MyItemLabel:
    instance = None

    def __init__(
        self, qnodes: Mapping[str, WDEntity], qnode_redirections: Mapping[str, str]
    ):
        self.qnodes = qnodes
        self.qnode_redirections = qnode_redirections

    @staticmethod
    def init(qnodes: Mapping[str, WDEntity], qnode_redirections: Mapping[str, str]):
        assert MyItemLabel.instance is None
        MyItemLabel.instance = MyItemLabel(qnodes, qnode_redirections)
        m_f.cf.m_item_labels = MyItemLabel.instance

    ###############################################################################
    # modified functions goes here

    def get_wd_qid_en(self, query, page_rank=True, get_qid=True):
        pass

    def get_wd_qid_all(self, query, page_rank=True, get_qid=True):
        pass

    ###############################################################################
    # below are functions that are copied from api/resources/m_item.py
