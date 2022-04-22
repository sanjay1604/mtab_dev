1. Entry of the application seems to start from `interface/routes.py`, the routes `/mtab/` point us to the function `api.annotator.main.annotate_api`. That function leads us to `api.annotator.main.run`
2. After preprocessing, in structure prediction, they predict headers of a table, which we already have, so we modify `m_structure.run` to use the given headers as well as options to pass the headers to it via parameter in `main.run`
3. My understanding about table headers are incorrect, it is actually the row indices containing table headers. Run again with list of rows with table column names and it is confirmed.
4. Next step is to override `m_semantic.generate_candidates` function to return the resolved entities. For that, we add another argument `table_links` to the `main.run` function
5. After overriding the candidate, the CEA is correct, but it doesn't do any CPA and CTA, needs to find out why.
6. In the CPA step (`log_m_cpa = "\nCPA:"`), they use property `p_cans` from previous CEA step. This leads us to the function `get_value_match` that we need to implement correctly.
   - implementing their database accessing method in `m_item.py`, checkout `m_parser_wikidata.py` so see how they process raw json dump of wikidata to the format
   - create our own implementation of `MItem` called `MyMItem`, and inject it after `m_f.init` so that `m_f.m_items()` can use `MyMItem`
   - in their parser (`m_parser_wikidata.py`), they use LANG = en, that affects extracting aliases and descriptions, so we follow them.
   - the code to build the whole db is in `MItem.build` and mainly in the commented next line: `MItem._build_from_dumps()`, in there we found that aliases contains label!
