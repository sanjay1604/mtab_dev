from typing import Dict, List, Mapping, Optional, Set, Tuple, TypedDict, Union

from api import m_f
from api.annotator import m_input
from kgdata.wikidata.models import WDEntity
from m_config import SourceType
from mtab_baseline.annotator.main import m_preprocess, m_structure, run
from mtab_baseline.resources.m_item import MyMItem
from sm.prelude import I

Output = TypedDict(
    "MTab",
    {
        "cpa": List[Tuple[int, int, str]],
        "cta": Dict[int, str],
        "cea": Dict[Tuple[int, int], str],
        "out": dict,
    },
)

TargetOutput = TypedDict(
    "TableTargetOutput",
    {
        "cpa": m_input.TargetCPA,
        "cta": m_input.TargetCTA,
        "cea": m_input.TargetCEA,
    },
)

Example = TypedDict(
    "Example",
    {
        "table": I.ColumnBasedTable,
        "links": Optional[Dict[Tuple[int, int], List[str]]],
        # the candidate entities for each link, this is optional but if provided,
        # mtab will use it instead of the ground-truth (prefer unlinked scenario than the linked scenario)
        # if provided, then the links must also be provided
        "candidates": Optional[Dict[Tuple[int, int], List[Tuple[str, float]]]],
        "subj_col": Optional[Tuple[int, str]],
    },
)

_InternalExample = TypedDict(
    "_InternalExample",
    {
        "table": I.ColumnBasedTable,
        "links": Optional[Dict[Tuple[int, int], List[str]]],
        "candidates": Optional[Dict[Tuple[int, int], List[Tuple[str, float]]]],
        "subj_col": Optional[Tuple[int, str]],
        "target_cpa": Optional[m_input.TargetCPA],
        "target_cta": Optional[m_input.TargetCTA],
        "target_cea": Optional[m_input.TargetCEA],
    },
)


has_init = False


def init_mtab():
    global has_init
    if not has_init:
        m_f.init(is_log=True)
        has_init = True


def predict(
    qnodes: Mapping[str, WDEntity],
    examples: List[Example],
    target_cpa_file: Optional[str],
    target_cta_file: Optional[str],
    target_cea_file: Optional[
        Union[
            str,
            Dict[
                str,
                List[Tuple[int, int, Optional[Set[str]]]],
            ],
            Dict[
                str,
                List[Tuple[int, int]],
            ],
        ]
    ],
    qnode_pageranks: Optional[Mapping[str, float]] = None,
) -> List[Output]:
    # init global variables
    init_mtab()
    MyMItem.init(qnodes, qnode_redirections={}, qnode_pageranks=qnode_pageranks or {})

    # parse the target cpa, cta, cea
    if target_cpa_file is not None:
        assert target_cta_file is not None
        if isinstance(target_cta_file, str):
            tar_cta, n_cta = m_input.parse_target_cta(target_cta_file)
        else:
            tar_cta = {}
            for table_id, lst in target_cta_file.items():
                tar_cta[table_id] = m_input.TargetCTA(table_id)
                for ci in lst:
                    tar_cta[table_id].add(ci)

        if isinstance(target_cpa_file, str):
            tar_cpa, n_cpa = m_input.parse_target_cpa(target_cpa_file)
        else:
            tar_cpa = {}
            for table_id, lst in target_cpa_file.items():
                tar_cpa[table_id] = m_input.TargetCPA(table_id)
                for ci, cj in lst:
                    tar_cpa[table_id].add(ci, cj)
    else:
        assert target_cta_file is None
        tar_cpa = None
        tar_cta = None

    if target_cea_file is not None:
        if isinstance(target_cea_file, str):
            tar_cea, n_cea = m_input.parse_target_cea(target_cea_file)
        else:
            assert isinstance(target_cea_file, dict)
            tar_cea = {}
            for table_id, lst in target_cea_file.items():
                tar_cea[table_id] = m_input.TargetCEA(table_id)
                for item in lst:
                    if len(item) == 2:
                        ri, ci, gt_cea = item[0], item[1], None
                    else:
                        ri, ci, gt_cea = item
                    tar_cea[table_id].add(ri + 1, ci, gt_cea)
    else:
        tar_cea = None

    new_examples: List[_InternalExample] = []
    for i, example in enumerate(examples):
        e: _InternalExample = {
            "table": example["table"],
            "links": example["links"],
            "candidates": example["candidates"],
            "subj_col": example["subj_col"],
            # "target_cea": target_cea,
            "target_cpa": tar_cpa.get(
                example["table"].table_id, m_input.TargetCPA(example["table"].table_id)
            )
            if tar_cpa is not None
            else None,
            "target_cta": tar_cta.get(
                example["table"].table_id, m_input.TargetCTA(example["table"].table_id)
            )
            if tar_cta is not None
            else None,
            "target_cea": tar_cea.get(
                example["table"].table_id, m_input.TargetCEA(example["table"].table_id)
            )
            if tar_cea is not None
            else None,
        }
        new_examples.append(e)

    outputs = []
    for newex in new_examples:
        outputs.append(_internal_predict(newex))

    # shutdown global variables
    MyMItem.deinit()
    return outputs


def predict_targets(
    tables: list[I.ColumnBasedTable],
) -> list[TargetOutput]:
    init_mtab()
    outputs: list[TargetOutput] = []
    for table in tables:
        table_name = table.table_id
        source_type = SourceType.OBJ
        source = convert_table(table)

        # copy most of the code from mtab_baseline.annotator.main.run
        table_obj = m_preprocess.run(source_type, source, table_name)
        m_structure.run(
            table_obj, tar_cea=None, tar_cta=None, tar_cpa=None, predict_target=True
        )
        tar_cea: m_input.TargetCEA = table_obj["tar"]["cea"]
        tar_cta: m_input.TargetCTA = table_obj["tar"]["cta"]
        tar_cpa: m_input.TargetCPA = table_obj["tar"]["cpa"]

        outputs.append({"cea": tar_cea, "cta": tar_cta, "cpa": tar_cpa})
    return outputs


def _internal_predict(example: _InternalExample):
    # if example["target_cpa"] is not None or example["target_cta"] is not None:
    #     target_cea = m_input.TargetCEA(example["table"].table_id)
    #     assert example["links"] is not None
    #     for (ri, ci), entity_id in example["links"].items():
    #         target_cea.add(ri + 1, ci, entity_id)
    # else:
    #     target_cea = None

    if example["candidates"] is not None:
        table_candidates = {
            (ri + 1, ci): lst for (ri, ci), lst in example["candidates"].items()
        }
    else:
        table_candidates = None

    if example["links"] is not None:
        table_links = {(ri + 1, ci): lst for (ri, ci), lst in example["links"].items()}
    else:
        table_links = None

    if example["subj_col"] is not None:
        table_core_attribute = example["subj_col"][0]
    elif example["target_cpa"] is not None:
        assert example["target_cta"] is not None
        # another way is to use the target_cpa and target_cta
        table_core_attribute = example["target_cpa"].core_attribute()
        if table_core_attribute is None:
            # (happen in semtab2020) okay we have a table (rarely happen) doesn't have any target properties
            # so we use the target cta to find the core attribute
            table_core_attribute = example["target_cta"].core_attribute()
        assert table_core_attribute is not None
    else:
        table_core_attribute = None

    out, runtime = run(
        source_type=SourceType.OBJ,
        source=convert_table(example["table"]),
        tar_cea=example["target_cea"],
        tar_cta=example["target_cta"],
        tar_cpa=example["target_cpa"],
        table_name=example["table"].table_id,
        table_headers=[0],
        table_links=table_links,
        table_candidates=table_candidates,
        table_core_attribute=table_core_attribute,
    )
    return process_output(out)


def process_output(out: dict) -> Output:
    cpa = []
    cta = {}
    cea = {}
    for tbl_id, source, target, props in out["res_cpa"]:
        prop = props[0]
        cpa.append((source, target, prop))

    for tbl_id, col, types in out["res_cta"]:
        type = types[0]
        cta[col] = type

    for tbl_id, ri, ci, entid in out["res_cea"]:
        cea[ri - 1, ci] = entid

    return {
        "cpa": cpa,
        "cta": cta,
        "cea": cea,
        "out": {k: v for k, v in out.items() if k not in {"__links", "tar"}},
    }


def convert_table(table: I.ColumnBasedTable):
    header = [col.name or f"column-{ci}" for ci, col in enumerate(table.columns)]
    rows = [[col.values[ri] for col in table.columns] for ri in range(table.shape()[0])]

    return [header] + rows
