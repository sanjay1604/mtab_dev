import sys
from pathlib import Path

root_dir = Path(__file__).absolute().parent.parent.parent

sys.path.append(str(root_dir))

from typing import Dict, List, Literal, Mapping, Optional, Tuple, TypedDict
from sm.prelude import I, M, O
from kgdata.wikidata.models import WDEntity
from m_config import SourceType
from tqdm.auto import tqdm
from api import m_f
from mtab_baseline.annotator.main import run
from sm.prelude import I, M, O
from mtab_baseline.resources.m_item import MyMItem
from kgdata.wikidata.db import get_entity_db, get_entity_redirection_db

Output = TypedDict(
    "MTab",
    {"cpa": List[Tuple[int, int, str]], "cta": Dict[int, str], "out": dict},
)


Example = TypedDict(
    "Example",
    {
        "table": I.ColumnBasedTable,
        "links": Dict[Tuple[int, int], List[str]],
        "subj_col": Optional[Tuple[int, str]],
    },
)


def predict(
    qnodes: Mapping[str, WDEntity],
    examples: List[Example],
) -> List[List[Output]]:
    if MyMItem.instance is None:
        m_f.init(is_log=True)
        MyMItem.init(qnodes, qnode_redirections={})

    # outputs = []
    # for example in tqdm(examples):
    #     table_links = {(ri + 1, ci): lst for (ri, ci), lst in example["links"].items()}
    #     out, runtime = run(
    #         source_type=SourceType.OBJ,
    #         source=convert_table(example["table"]),
    #         table_name=example["table"].table_id,
    #         table_headers=[0],
    #         table_links=table_links,
    #         table_core_attribute=example["subj_col"][0]
    #         if example["subj_col"] is not None
    #         else None,
    #     )

    #     outputs.append(process_output(out))
    outputs = []
    outputs.append(predict_one_example(examples[0]))
    outputs += M.parallel_map(predict_one_example, examples[1:], show_progress=True)
    return outputs


def predict_one_example(example: Example):
    table_links = {(ri + 1, ci): lst for (ri, ci), lst in example["links"].items()}
    out, runtime = run(
        source_type=SourceType.OBJ,
        source=convert_table(example["table"]),
        table_name=example["table"].table_id,
        table_headers=[0],
        table_links=table_links,
        table_core_attribute=example["subj_col"][0]
        if example["subj_col"] is not None
        else None,
    )
    return process_output(out)


def process_output(out: dict) -> Output:
    cpa = []
    cta = {}
    for tbl_id, source, target, props in out["res_cpa"]:
        prop = props[0]
        cpa.append((source, target, prop))

    for tbl_id, col, types in out["res_cta"]:
        type = types[0]
        cta[col] = type

    return {
        "cpa": cpa,
        "cta": cta,
        "out": {k: v for k, v in out.items() if k not in {"__links", "tar"}},
    }


def convert_table(table: I.ColumnBasedTable):
    header = [col.name or f"column-{ci}" for ci, col in enumerate(table.columns)]
    rows = [[col.values[ri] for col in table.columns] for ri in range(table.shape()[0])]

    return [header] + rows
