import os
import subprocess
import sys
from pathlib import Path
import shutil

root_dir = Path(__file__).absolute().parent.parent.parent

sys.path.append(str(root_dir))

from typing import Dict, List, Literal, Mapping, Optional, Tuple, TypedDict
from sm.prelude import I, M, O
from kgdata.wikidata.models import QNode
from m_config import SourceType
from tqdm.auto import tqdm
from api import m_f
from mtab_baseline.annotator.main import run
from sm.prelude import I, M, O
from mtab_baseline.resources.m_item import MyMItem
from kgdata.wikidata.db import get_qnode_db, get_qnode_redirection_db
from mtab_baseline.main import Output, Example, convert_table


def predict(examples: List[Example], cache_dir: Path) -> List[List[Output]]:
    outputs = M.parallel_map(
        predict_one_table,
        inputs=[(example["table"], cache_dir) for example in examples],
        show_progress=True,
        use_threadpool=True,
        n_processes=6,
    )
    # outputs = predict_tables([e["table"] for e in examples], cache_dir)
    return outputs


def predict_one_table(table: I.ColumnBasedTable, cache_dir: Path):
    content = convert_table(table)
    table_fsname = (
        table.table_id.replace("/", "_")
        .replace("'", "_")
        .replace('"', "_")
        .replace(",", "_")
    )
    cwd = cache_dir / table_fsname
    cwd.mkdir(exist_ok=True, parents=True)
    infile = cwd / (table_fsname + ".csv")
    outfile = cwd / "outfile.json"

    # just to make sure that table_fsname = table_id
    idfile = cwd / (table_fsname + ".txt")
    if idfile.exists():
        assert M.deserialize_text(idfile).strip() == table.table_id
    else:
        M.serialize_text(table.table_id, idfile)

    rerun = True
    if infile.exists() and outfile.exists():
        M.serialize_csv(content, str(infile) + ".tmp")
        if M.deserialize_text(infile) == M.deserialize_text(str(infile) + ".tmp"):
            rerun = False

    if rerun:
        M.serialize_csv(content, infile)
        try:
            subprocess.check_call(
                [
                    "curl",
                    "-X",
                    "POST",
                    "-F",
                    f"file=@{str(infile)}",
                    "https://mtab.app/api/v1/mtab?limit=1000",
                    "-o",
                    outfile,
                ]
            )
        except:
            if os.path.exists(outfile):
                os.remove(outfile)
            raise

    record = M.deserialize_json(outfile)
    assert record["status"] == "Success"

    semantic = record["tables"][0]["semantic"]

    assert record["tables"][0]["name"] == table_fsname
    cpa, cta = [], {}
    for item in semantic["cpa"]:
        source, target = item["target"]
        # assert len(item["annotation"]) == 1
        uri = item["annotation"][0]["wikidata"]
        label = item["annotation"][0]["label"]
        assert uri.startswith("http://www.wikidata.org/prop/direct/")
        pid = uri.replace("http://www.wikidata.org/prop/direct/", "")
        cpa.append((source, target, pid))

    for item in semantic["cta"]:
        # assert len(item["annotation"]) == 1
        uri = item["annotation"][0]["wikidata"]
        label = item["annotation"][0]["label"]
        assert uri.startswith("http://www.wikidata.org/entity/")
        qid = uri.replace("http://www.wikidata.org/entity/", "")
        cta[item["target"]] = qid

    return {"cpa": cpa, "cta": cta, "out": record["tables"][0]}


def predict_tables(tables: List[I.ColumnBasedTable], cache_dir: Path):
    # first of all, generate a unique id for this batch
    batch_nos = (
        [
            int(dir.name.replace("batch_", ""))
            for dir in cache_dir.iterdir()
            if dir.name.startswith("batch_")
        ]
        if cache_dir.exists()
        else []
    )

    if len(batch_nos) == 0:
        batch_id = f"batch_000"
    else:
        batch_id = f"batch_{max(batch_nos) + 1:03d}"

    batch_dir = cache_dir / batch_id

    outfile = batch_dir / "outfile.json"
    if not outfile.exists():
        # write tables to that directory and create a zip file
        (batch_dir / "upload/tables").mkdir(exist_ok=True, parents=True)
        for table in tables:
            content = convert_table(table)
            M.serialize_csv(
                content, batch_dir / "upload/tables" / (table.table_id + ".csv")
            )

        shutil.make_archive(str(batch_dir / "tables"), "zip", batch_dir / "upload")

        try:
            subprocess.check_call(
                [
                    "curl",
                    "-X",
                    "POST",
                    "-F",
                    f"file=@{str(batch_dir / 'tables.zip')}",
                    "https://mtab.app/api/v1/mtab?limit=1000",
                    "-o",
                    outfile,
                ]
            )
        except:
            if os.path.exists(outfile):
                os.remove(outfile)
            raise

    record = M.deserialize_json(outfile)
    assert record["status"] == "Success"

    assert False
    semantic = record["tables"][0]["semantic"]

    cpa, cta = [], {}
    for item in semantic["cpa"]:
        source, target = item["target"]
        # assert len(item["annotation"]) == 1
        uri = item["annotation"][0]["wikidata"]
        label = item["annotation"][0]["label"]
        assert uri.startswith("http://www.wikidata.org/prop/direct/")
        pid = uri.replace("http://www.wikidata.org/prop/direct/", "")
        cpa.append((source, target, pid))

    for item in semantic["cta"]:
        # assert len(item["annotation"]) == 1
        uri = item["annotation"][0]["wikidata"]
        label = item["annotation"][0]["label"]
        assert uri.startswith("http://www.wikidata.org/entity/")
        qid = uri.replace("http://www.wikidata.org/entity/", "")
        cta[item["target"]] = qid
