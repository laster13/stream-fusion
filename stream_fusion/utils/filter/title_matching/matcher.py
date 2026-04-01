from RTN import title_match

from stream_fusion.logging_config import logger
from stream_fusion.utils.filter.title_matching.normalizer import TitleNormalizer


class TitleMatcher:
    """
    Matches a torrent title against a list of TMDB titles using 5 progressive levels.

    Levels:
      1. exact_normalized        — identical after normalize()
      2. ordered_subset          — item words ⊂ TMDB words (guard: ≥ 2 item words)
      3. reverse_ordered_subset  — TMDB words ⊂ item words (guard: ≥ 2 TMDB words)
      4. fuzzy_rtn               — Levenshtein via RTN (guard: ≥ 2 words on either side)
      5. article_stripped        — exact match after stripping leading articles
    """

    def __init__(self, normalizer: TitleNormalizer) -> None:
        self._n = normalizer
        self._rtn_cache: dict[tuple[str, str], bool] = {}

    # ── Core matching logic ──────────────────────────────────────────────────

    def match(
        self, item_raw: str, tmdb_titles: list[str]
    ) -> tuple[bool, str | None, str | None]:
        """
        Check whether item_raw matches any of tmdb_titles.

        Returns:
            (matched, reason, matched_with)
        """
        n = self._n

        # Prepare item side
        raw = self._n.remove_integrale(item_raw)
        cleaned_item = n.clean_release_title(raw)
        normalized_item = n.normalize(cleaned_item)
        item_words = normalized_item.split()
        stripped_item = n.strip_leading_article(normalized_item)

        # Prepare TMDB side (pre-compute once per call)
        precomputed: list[dict] = []
        for t in tmdb_titles:
            cleaned_t = n.remove_integrale(n.clean_tmdb_title(t))
            normalized_t = n.normalize(cleaned_t)
            precomputed.append({
                "original": t,
                "normalized": normalized_t,
                "words": normalized_t.split(),
                "stripped": n.strip_leading_article(normalized_t),
            })

        for td in precomputed:
            normalized_title = td["normalized"]
            title_words = td["words"]
            stripped_title = td["stripped"]

            # Level 1: exact normalized
            if normalized_item == normalized_title:
                return True, "exact_normalized", td["original"]

            # Level 2: item ⊂ TMDB (ordered subset) — guard ≥ 2 item words
            if len(item_words) >= 2 and _is_ordered_subset(item_words, title_words):
                return True, "ordered_subset", td["original"]

            # Level 3: TMDB ⊂ item (reverse ordered subset) — guard ≥ 2 TMDB words
            if len(title_words) >= 2 and _is_ordered_subset(title_words, item_words):
                return True, "reverse_ordered_subset", td["original"]

            # Level 4: fuzzy RTN (Levenshtein) — guard ≥ 2 words on either side
            if len(title_words) >= 2 or len(item_words) >= 2:
                rtn_key = (normalized_title, normalized_item)
                if rtn_key not in self._rtn_cache:
                    self._rtn_cache[rtn_key] = title_match(normalized_title, normalized_item)
                if self._rtn_cache[rtn_key]:
                    return True, "fuzzy_rtn", td["original"]

            # Level 5: article stripped exact match
            if stripped_item and stripped_title and stripped_item == stripped_title:
                return True, "article_stripped", td["original"]

            # Level 5b: article stripped ordered subset — guard ≥ 2 words
            stripped_item_words = stripped_item.split()
            stripped_title_words = stripped_title.split()
            if (
                len(stripped_item_words) >= 2
                and _is_ordered_subset(stripped_item_words, stripped_title_words)
            ):
                return True, "article_stripped_subset", td["original"]

        return False, None, None

    def filter_items(self, items: list, titles: tuple[str, ...]) -> list:
        """Filter a list of TorrentItem, keeping those that match any of the TMDB titles."""
        tmdb_titles = list(titles)
        filtered = []
        for item in items:
            if hasattr(item, "_ensure_parsed_data_valid"):
                item._ensure_parsed_data_valid()

            parsed_title = (
                item.parsed_data.parsed_title
                if (item.parsed_data and hasattr(item.parsed_data, "parsed_title"))
                else None
            )
            item_raw = parsed_title or getattr(item, "raw_title", "")

            matched, reason, matched_with = self.match(item_raw, tmdb_titles)

            if matched:
                logger.trace(
                    f"KEEP TITLE | raw_title={getattr(item, 'raw_title', None)} | "
                    f"parsed_title={parsed_title} | "
                    f"matched_with={matched_with} | reason={reason}"
                )
                filtered.append(item)
            else:
                logger.trace(
                    f"REJECT TITLE | raw_title={getattr(item, 'raw_title', None)} | "
                    f"parsed_title={parsed_title} | candidate_titles={tmdb_titles}"
                )

        logger.debug(
            f"TitleMatcher: kept={len(filtered)} rejected={len(items) - len(filtered)}"
        )
        return filtered

    def analyze(self, raw_title: str, tmdb_titles: list[str]) -> dict:
        """
        Detailed analysis for the admin test page.
        Returns normalization steps + per-title matching breakdown.
        """
        n = self._n

        # Normalization steps
        try:
            from RTN import parse
            parsed = parse(raw_title)
            parsed_title = getattr(parsed, "parsed_title", None)
        except Exception:
            parsed_title = None

        steps = n.analyze_steps(raw_title, parsed_title)

        # Per-title match analysis
        title_results = []
        matched_overall = False
        matched_reason = None
        matched_with = None

        raw_for_match = parsed_title or raw_title
        item_raw = n.remove_integrale(raw_for_match)
        cleaned_item = n.clean_release_title(item_raw)
        normalized_item = n.normalize(cleaned_item)
        item_words = normalized_item.split()
        stripped_item = n.strip_leading_article(normalized_item)

        for t in tmdb_titles:
            if not t.strip():
                continue
            cleaned_t = n.remove_integrale(n.clean_tmdb_title(t))
            normalized_t = n.normalize(cleaned_t)
            title_words = normalized_t.split()
            stripped_title = n.strip_leading_article(normalized_t)

            levels = {}
            # L1
            l1 = normalized_item == normalized_t
            levels["exact_normalized"] = l1
            if l1 and not matched_overall:
                matched_overall, matched_reason, matched_with = True, "exact_normalized", t

            # L2
            l2 = len(item_words) >= 2 and _is_ordered_subset(item_words, title_words)
            levels["ordered_subset"] = l2
            if l2 and not matched_overall:
                matched_overall, matched_reason, matched_with = True, "ordered_subset", t

            # L3
            l3 = len(title_words) >= 2 and _is_ordered_subset(title_words, item_words)
            levels["reverse_ordered_subset"] = l3
            if l3 and not matched_overall:
                matched_overall, matched_reason, matched_with = True, "reverse_ordered_subset", t

            # L4
            rtn_result = False
            if len(title_words) >= 2 or len(item_words) >= 2:
                rtn_key = (normalized_t, normalized_item)
                if rtn_key not in self._rtn_cache:
                    self._rtn_cache[rtn_key] = title_match(normalized_t, normalized_item)
                rtn_result = self._rtn_cache[rtn_key]
            levels["fuzzy_rtn"] = rtn_result
            if rtn_result and not matched_overall:
                matched_overall, matched_reason, matched_with = True, "fuzzy_rtn", t

            # L5
            l5 = bool(stripped_item and stripped_title and stripped_item == stripped_title)
            levels["article_stripped"] = l5
            if l5 and not matched_overall:
                matched_overall, matched_reason, matched_with = True, "article_stripped", t

            # L5b
            si_words = stripped_item.split()
            st_words = stripped_title.split()
            l5b = len(si_words) >= 2 and _is_ordered_subset(si_words, st_words)
            levels["article_stripped_subset"] = l5b
            if l5b and not matched_overall:
                matched_overall, matched_reason, matched_with = True, "article_stripped_subset", t

            title_results.append({
                "tmdb_title": t,
                "normalized": normalized_t,
                "stripped": stripped_title,
                "levels": levels,
                "matched": any(levels.values()),
                "reason": next((k for k, v in levels.items() if v), None),
            })

        return {
            "steps": steps,
            "normalized_item": normalized_item,
            "stripped_item": stripped_item,
            "title_results": title_results,
            "matched": matched_overall,
            "reason": matched_reason,
            "matched_with": matched_with,
        }


def _is_ordered_subset(subset_words: list[str], full_words: list[str]) -> bool:
    """Return True if subset_words appear in order within full_words."""
    it = iter(full_words)
    return all(w in it for w in subset_words)
