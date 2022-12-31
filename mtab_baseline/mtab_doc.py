from typing_extensions import Self

import api.annotator.m_preprocess
import api.lookup.m_entity_bm25
import api.lookup.m_entity_fuzzy
import api.lookup.m_entity_search
import api.utilities.m_utils
import mtab_baseline
import mtab_baseline.annotator
import mtab_baseline.annotator.m_semantic
import mtab_baseline.annotator.m_structure
import mtab_baseline.annotator.main
import mtab_baseline.main

# fmt: off

class begin:
  pass
class end:
  pass
class remain:
  pass


class n:
  def __init__(self, *args):
    pass

  def step(self, *args) -> Self:
    return self

  def nstep(self, *args) -> Self:
    """Nested step"""
    return self

(
  n(mtab_baseline.main.predict, "Main Entrance of MTab, do the prediction for multiple tables")
  .step(begin, 64, "gather target cpa & cta")
  .step(remain, "for each table calls", mtab_baseline.main._internal_predict, "to predict for one table")
)

(
  n(mtab_baseline.main._internal_predict, "Do the prediction for one table.")
  .step(begin, 98, "generate target CEA for each link if target CPA & CTA are given")
  .step(102, 113, "if provided by users, give MTab the table's core attribute")
  .step(remain, "call", mtab_baseline.annotator.main.run, "to do the real prediction")
)

(
  n(mtab_baseline.annotator.main.run, "do the prediction for one table")
  .step(begin, 51, "call", api.annotator.m_preprocess.run, "do preprocessing for structure prediction step that figure out format, encoding, core attributes, headers and passdown __links and __candidates")
  .step(55, "call", mtab_baseline.annotator.m_structure.run, "do the structure prediction: headers, column types, core attributes, and targets")
  .step(64, "call", mtab_baseline.annotator.m_semantic.run, "do semantic annotation (the prediction -- final output)")
)

(
  n(mtab_baseline.annotator.m_structure.run, "predicting the structure of the table")
  .step(360, "call", mtab_baseline.annotator.m_structure.predict_targets, "predict target cea, cta, and cpa")
)

(
  n(mtab_baseline.annotator.m_structure.predict_targets, "predicting the target cea, cta, and cpa of the table")
  .step(begin, 264, "test if we need to predict the targets. MTab code is to only perform the check if three targets are not provided. We modify it to predict if any of the target is not provided, as their later logic does only predict the missing targets")
)

(
  n(mtab_baseline.annotator.m_semantic.run, "semantic annotation -- do the prediction for one table")
  .step(begin, 344, "generate candidate entities")
  .nstep(340, "call", mtab_baseline.annotator.m_semantic.generate_candidates_from_given_lists, "if __candidates (our modification for unlinked tables) are provided")
  .nstep(342, "call", mtab_baseline.annotator.m_semantic.generate_candidates_from_links, "if __links (our modification for linked tables) are provided")
  .nstep(344, "call", mtab_baseline.annotator.m_semantic.generate_candidates, "if __links are not provided -- mtab original code to generate candidate entities")
)

(
  n(mtab_baseline.annotator.m_semantic.generate_candidates, "generate candidate entities by m_entity_search")
  .step(293, "search for the candidates of a cell at row r_i, col c_i of the predicted cea target by calling", api.lookup.m_entity_search.search)
)

(
  n(api.lookup.m_entity_search.search, "search candidate entities given a query")
  .step(begin, 104, "if the query is entity id, return immediately")
  .step(106, 128, "the query is not entity id, normalize the query before pass it to elasticsearch")
  .step(192, "if the search mode is b, search using", api.lookup.m_entity_bm25.ESearch.search_wd)
  .step(133, "if the search mode is f, search using", api.lookup.m_entity_fuzzy.FuzzySearch.search_wd, "which use symspell to search within an edit distance")
  .step(137, 150, "if the search mode is a (which is the default), combines the results of",
    api.lookup.m_entity_bm25.ESearch.search_wd,
    api.lookup.m_entity_fuzzy.FuzzySearch.search_wd,
    "and merge the ranking using", api.utilities.m_utils.merge_ranking
  )
  .step(157, "if there is some results, call local func: get_wd_score, which is going to return the final output")
)

(
  n(api.lookup.m_entity_bm25.ESearch.search_wd, "search entities given a query")
  .step(196, "call", api.lookup.m_entity_bm25.ESearch.search_label)
  .step(200, "if there is no result, and query has ( or [ call", api.lookup.m_entity_bm25.ESearch.search_label, "without the value in the parenthesis or brackets")
  .step(216, 226, 'if the query contains ("<content>"), then call', api.lookup.m_entity_bm25.ESearch.search_label, "with <content> and add the result to the final result if **the result is not in** the final result")
  .step(229, 241, 'if the query contains ["<content>"], then call', api.lookup.m_entity_bm25.ESearch.search_label, "with <content> and the score of the result is multiplied with a discount factor 0.99 and add to the final result (if the same entity exists, choose the higher score)")
  .step(remain, "sort and return the final result with the given limit")
)

(
  n(api.lookup.m_entity_bm25.ESearch.search_label, "search entities by a query")
  .step(begin, 119, "normalize the query")
  .step(121, 156, "define a combine_results function, which execute the query (fuzzy or match) and return a dictionary which score is normalized between [0, 1] (by minus the small value and divided by the range")
  .step(158, "call combine_results to get the non-fuzzy results")
  .step(164, 175, "if fuzzy search is enabled, also do fuzzy search, and combine two results, if an entity is in both results, use the higher score")
)
