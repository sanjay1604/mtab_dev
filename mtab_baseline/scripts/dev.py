"""Script to develop mtab baseline"""

import sys
from pathlib import Path

root_dir = Path(__file__).absolute().parent.parent.parent

sys.path.append("/workspace/sm-dev")
sys.path.append(str(root_dir))

from m_config import SourceType

from api import m_f
from mtab_baseline.annotator.main import run
from sm.prelude import I, M, O
from mtab_baseline.resources.m_item import MyMItem
from kgdata.wikidata.db import get_entity_db, get_entity_redirection_db

data_dir = Path("/workspace/sm-dev/data/home/cache/t211229_baselines/wt250")

index = M.deserialize_json(data_dir / "index.json")
tables = index["tables"]

idx = 8
table_id, table_fsname = tables[idx]
table_file = data_dir / f"inputs/s{idx:03d}_{table_fsname}.json.gz"
example = M.deserialize_json(table_file)
table = I.ColumnBasedTable.from_dict(example["table"])
qnodes = get_entity_db(
    index["qnodes"], create_if_missing=False, read_only=True, proxy=False
)
qnode_redirections = get_entity_redirection_db(
    index["qnodes"].replace("qnodes.db", "qnode_redirections.db"),
    create_if_missing=False,
    read_only=True,
)

table_links = {}
for ri, ci, lst in example["links"]:
    # ri + 1 due to the header added to the first row
    table_links[ri + 1, ci] = [
        x["entity_id"] for x in lst if x["entity_id"] is not None
    ]


def convert_table(table: I.ColumnBasedTable):
    header = [col.name or f"column-{ci}" for ci, col in enumerate(table.columns)]
    rows = [[col.values[ri] for col in table.columns] for ri in range(table.shape()[0])]

    return [header] + rows


m_f.init(is_log=True)
MyMItem.init(qnodes.cache(), qnode_redirections)

result, runtime = run(
    source_type=SourceType.OBJ,
    source=convert_table(table),
    table_name=table.table_id,
    table_headers=[0],
    table_links=table_links,
)
print(result["log"])
