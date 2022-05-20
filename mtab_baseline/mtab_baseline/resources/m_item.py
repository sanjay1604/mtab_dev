from collections import Counter, defaultdict
from typing import IO, Mapping, cast
from kgdata.wikidata.models import (
    WDEntity,
)
from numpy import isin
from api import m_f
import unicodedata
import m_config


class MyMItem:
    instance = None

    def __init__(
        self, qnodes: Mapping[str, WDEntity], qnode_redirections: Mapping[str, str]
    ):
        self.qnodes = qnodes
        self.qnode_redirections = qnode_redirections

    @staticmethod
    def init(qnodes: Mapping[str, WDEntity], qnode_redirections: Mapping[str, str]):
        assert MyMItem.instance is None
        MyMItem.instance = MyMItem(qnodes, qnode_redirections)
        m_f.cf.m_wiki_items = MyMItem.instance

    ###############################################################################
    # modified functions goes here
    def get_facts_literal(self, wd_id: str):
        outs = {
            "string": defaultdict(set),
            "time": defaultdict(set),
            "quantity": defaultdict(set),
        }

        if wd_id not in self.qnodes:
            return None

        qnode = self.qnodes[wd_id]
        for prop, stmts in qnode.props.items():
            for stmt in stmts:
                if stmt.value.is_time(stmt.value):
                    claim_value = stmt.value.value["time"]
                    claim_value = claim_value.replace("T00:00:00Z", "")
                    if claim_value[0] == "+":
                        claim_value = claim_value[1:]
                    outs["time"][prop].add(claim_value)
                elif stmt.value.is_quantity(stmt.value):
                    claim_unit = stmt.value.value["unit"]
                    claim_unit = claim_unit.replace(m_config.WD, "")

                    claim_value = stmt.value.value["amount"]
                    if claim_value[0] == "+":
                        claim_value = claim_value[1:]

                    outs["quantity"][prop].add((claim_value, claim_unit))
                elif stmt.value.is_string(stmt.value):
                    claim_value = stmt.value.value
                    outs["string"][prop].add(claim_value)
                elif stmt.value.is_mono_lingual_text(stmt.value):
                    claim_value = stmt.value.value["text"]
                    outs["string"][prop].add(claim_value)
                else:
                    # they don't handle coordinates
                    assert stmt.value.is_entity_id(
                        stmt.value
                    ) or stmt.value.is_globe_coordinate(stmt.value), stmt.value
        return outs

    def get_label(self, wd_id):
        qnode = self.qnodes[wd_id]
        respond = str(qnode.label)
        # if "\u200e" in respond:
        #     debug = 1
        if respond and isinstance(respond, str):
            respond = "".join(
                filter(lambda c: unicodedata.category(c) != "Cf", respond)
            )
            # respond = "".join(filter(lambda x: x in string.printable, respond))
            # respond = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", respond)
            # respond = (
            #     respond.encode("ascii", errors="ignore")
            #     .strip()
            #     .decode("unicode_escape")
            # )
        return respond

    def get_labels(self, wd_id: str, multilingual: bool = False):
        responds = set()
        tmp = self.get_aliases(wd_id)
        if tmp:
            responds.update(set(tmp))
        if multilingual:
            tmp = self.get_aliases_multilingual(wd_id)
            if tmp:
                responds.update(set(tmp))

        # remove unicode text
        new_responds = set()
        for respond in responds:
            if respond:
                new_respond = "".join(
                    filter(lambda c: unicodedata.category(c) != "Cf", respond)
                )
                # new_respond = "".join(filter(lambda x: x in string.printable, respond))
                # new_respond = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", respond)
                # new_respond = (
                #     respond.encode("ascii", errors="ignore")
                #     .strip()
                #     .decode("unicode_escape")
                # )
                if new_respond:
                    new_responds.add(new_respond)
        return new_responds

    def get_aliases(self, wd_id: str):
        # ALIASES INCLUDE THE LABEL, checkout `MItem.build` and `MItem._build_from_dumps` for more information
        # language is en, so our default and their default are matched
        if wd_id not in self.qnodes:
            return None
        qnode = self.qnodes[wd_id]
        out = {str(qnode.label)}
        out.update(qnode.aliases.lang2values.get(qnode.aliases.lang, []))
        return out

    def get_aliases_multilingual(self, wd_id: str):
        # ALIASES INCLUDE THE LABEL, checkout `MItem.build` and `MItem._build_from_dumps` for more information
        if wd_id not in self.qnodes:
            return None
        qnode = self.qnodes[wd_id]
        out = set(qnode.label.lang2value.values())
        for lst in qnode.aliases.lang2values.values():
            out.update(lst)
        return out

    def get_tail_entity(self, wd_id, get_values=True):
        outs = defaultdict(set)
        if wd_id not in self.qnodes:
            return None

        qnode = self.qnodes[wd_id]
        for prop, stmts in qnode.props.items():
            for stmt in stmts:
                if stmt.value.is_entity_id(stmt.value):
                    claim_value = stmt.value.value["id"]
                    outs[prop].add(claim_value)
        return outs

    def get_facts_entities_others(self, wd_id):
        # DO NOT KNOW HOW TO GET THE OTHER ENTITY DB -- seems like it's entities in dbpedia and wikipedia
        return {}

    def get_instance_of(self, wd_id, get_label=False):
        qnode = self.qnodes[wd_id]
        qnode_types = [
            stmt.value.as_entity_id()
            for stmt in qnode.props.get("P31", [])
            if stmt.value.is_entity_id(stmt.value)
        ]

        if get_label:
            return {qid: self.get_label(qid) for qid in qnode_types}
        else:
            return qnode_types

    def get_subclass_of(self, wd_id, get_label=False):
        qnode = self.qnodes[wd_id]
        qnode_types = [
            stmt.value.as_entity_id()
            for stmt in qnode.props.get("P279", [])
            if stmt.value.is_entity_id(stmt.value)
        ]

        if get_label:
            return {qid: self.get_label(qid) for qid in qnode_types}
        else:
            return qnode_types

    ###############################################################################
    # below are functions that are copied from api/resources/m_item.py

    def get_statement_values(self, wd_id, multilingual=False):
        statements = {
            "num": defaultdict(set),
            "text": defaultdict(set),
            "entity": defaultdict(set),
        }

        claims_literal = self.get_facts_literal(wd_id)
        if claims_literal:
            for prop, values in claims_literal["quantity"].items():
                for value_text in values:
                    if isinstance(value_text, tuple) or isinstance(value_text, list):
                        value_text = value_text[0]
                    statements["num"][value_text].add(prop)

            text_statements = {
                **claims_literal["string"],
                **claims_literal["time"],
            }
            for prop, values in text_statements.items():
                for value_text in values:
                    if isinstance(value_text, tuple) or isinstance(value_text, list):
                        value_text = value_text[0]
                    statements["text"][value_text].add(prop)

        def update_entity_label(claims_wd):
            if claims_wd:
                for prop, wd_id_set in claims_wd.items():
                    if "Section: " in prop:
                        continue
                    for e_id in wd_id_set:
                        # if get_labels:
                        #     e_label = self.get_labels(e_id, default_return="")
                        # else:
                        #     e_label = ""
                        tmp_labels = self.get_labels(e_id, multilingual=multilingual)
                        if not tmp_labels:
                            continue
                        for e_label in tmp_labels:
                            statements["entity"][e_label].add((prop, e_id))

        update_entity_label(self.get_tail_entity(wd_id))
        update_entity_label(self.get_facts_entities_others(wd_id))

        # # add P31 type of
        # claim_p31p279 = self.get_types_all(wd_id)
        # if claim_p31p279:
        #     for e_id in claim_p31p279:
        #         e_label = self.get_label(e_id)
        #         if not e_label:
        #             e_label = ""
        #         statements["entity"][e_label].add(("P31", e_id))
        #
        # # add P361 part of
        # claim_p361 = self.get_part_of_all(wd_id)
        # for e_id in claim_p361:
        #     e_label = self.get_label(e_id)
        #
        #     if not e_label:
        #         e_label = ""
        #     statements["entity"][e_label].add(("P361", e_id))
        return statements

    def get_p279_distance(self, wd_id, cursor_distance=0, max_distance=5):
        if cursor_distance == max_distance:
            return Counter()
        responds = self.get_subclass_of(wd_id)
        if responds:
            responds = {
                respond: cursor_distance + 1 for respond in responds
            }  # if respond not in st.WD_TOP}
        return responds

    def get_p279_all_distances(self, wd_id):
        # return self._get_attribute(self._db_p279, wd_id)
        parents = Counter()
        cursor = self.get_p279_distance(wd_id, cursor_distance=0)
        while cursor:
            for respond, score in cursor.items():
                if not parents.get(respond):
                    parents[respond] = score
                else:
                    parents[respond] = min(parents[respond], score)

            tmp_cursor = Counter()
            for tmp_id, score1 in cursor.items():
                tmp_p279 = self.get_p279_distance(tmp_id, cursor_distance=score1)
                if tmp_p279:
                    for respond, score2 in tmp_p279.items():

                        if not tmp_cursor.get(respond) and not parents.get(respond):
                            tmp_cursor[respond] = score2

                        if parents.get(respond):
                            parents[respond] = min(parents[respond], score2)

                        if tmp_cursor.get(respond):
                            tmp_cursor[respond] = min(tmp_cursor[respond], score2)
            cursor = tmp_cursor
        return parents

    def get_lowest_types(self, type_id_list):
        if len(type_id_list) == 1:
            return type_id_list
        lowest_type = set()
        parents_distance = defaultdict()
        for a_type in type_id_list:
            if any(True for parents in parents_distance.values() if a_type in parents):
                continue
            parents_types = self.get_p279_all_distances(a_type)
            parents_distance[a_type] = parents_types
            if lowest_type:
                is_add = True
                if parents_types:
                    for parent_type in parents_types:
                        if parent_type in lowest_type:
                            lowest_type.remove(parent_type)
                            lowest_type.add(a_type)
                            is_add = False
                if is_add:
                    lowest_type.add(a_type)
                # if any(
                #     a_type in parents_distance[_a_type].keys()
                #     for _a_type in lowest_type
                # ):
                #     lowest_type.add(a_type)
            else:
                lowest_type.add(a_type)

        # if len(lowest_type) == 1:
        #     lowest_type = list(lowest_type)[0]
        # else:
        #     # lowest_type = {c_type: f.wikidata_info().get_popularity(c_type) for c_type in lowest_type}
        #     # lowest_type = sorted(lowest_type.items(), key=lambda x: x[1], reverse=True)
        #     # lowest_type = lowest_type[0][0]
        #     # else:
        #     #     cta_final = c_types_direct_dup[0]
        #     #
        #     if "Q20181813" in lowest_type:
        #         lowest_type = "Q20181813"
        #     elif "Q3624078" in lowest_type:
        #         lowest_type = "Q3624078"
        #     else:
        #         try:
        #             # Get common parents:
        #             parents_common = Counter()
        #             score_common = defaultdict(int)
        #             for a_type in lowest_type:
        #                 for parents, distance in parents_distance[a_type].items():
        #                     parents_common[parents] += 1
        #                     score_common[parents] += distance
        #             parents_common_max = max(parents_common.values())
        #             parents_common = [
        #                 [a_type, score_common[a_type]]
        #                 for a_type, count in parents_common.items()
        #                 if count == parents_common_max
        #             ]
        #             if len(parents_common):
        #                 parents_common.sort(key=lambda x: x[1])
        #                 if parents_common[0][1] * 1.0 / parents_common_max < 3:
        #                     lowest_type = parents_common[0][0]
        #         except Exception as message:
        #             iw.print_status(message)
        #
        # if not isinstance(lowest_type, str):
        #     # Get oldest id
        #     # lowest_type = ul.get_oldest(list(lowest_type))
        #     lowest_type = None

        return list(lowest_type)
