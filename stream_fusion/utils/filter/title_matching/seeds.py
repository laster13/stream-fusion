"""
Default seed data for title_normalization_rules and language_rules tables.
Populated at first startup when the tables are empty.
Mirrors all previously hardcoded values from:
  - stream_fusion/utils/filter_results.py (_RELEASE_TAGS_PATTERN, etc.)
  - stream_fusion/constants.py (FRENCH_PATTERNS, FR_RELEASE_GROUPS)
  - stream_fusion/utils/filter/language_priority_filter.py (lang_mapping, priority_groups)
"""

# ── Title normalization rules ─────────────────────────────────────────────────

DEFAULT_TITLE_RULES: list[dict] = [
    # Substitutions
    {"rule_type": "substitution", "pattern": "&",  "replacement": "and", "description": "Esperluette → and"},
    {"rule_type": "substitution", "pattern": "+",  "replacement": "and", "description": "Plus → and"},
    {"rule_type": "substitution", "pattern": "@",  "replacement": "at",  "description": "Arobase → at"},

    # Ligatures
    {"rule_type": "ligature", "pattern": "œ", "replacement": "oe", "description": "Ligature OE minuscule"},
    {"rule_type": "ligature", "pattern": "Œ", "replacement": "oe", "description": "Ligature OE majuscule"},
    {"rule_type": "ligature", "pattern": "æ", "replacement": "ae", "description": "Ligature AE minuscule"},
    {"rule_type": "ligature", "pattern": "Æ", "replacement": "ae", "description": "Ligature AE majuscule"},

    # Articles (leading articles to ignore when comparing)
    {"rule_type": "article", "pattern": "the",  "replacement": "", "description": "Article EN"},
    {"rule_type": "article", "pattern": "a",    "replacement": "", "description": "Article EN"},
    {"rule_type": "article", "pattern": "an",   "replacement": "", "description": "Article EN"},
    {"rule_type": "article", "pattern": "le",   "replacement": "", "description": "Article FR"},
    {"rule_type": "article", "pattern": "la",   "replacement": "", "description": "Article FR"},
    {"rule_type": "article", "pattern": "les",  "replacement": "", "description": "Article FR"},
    {"rule_type": "article", "pattern": "l",    "replacement": "", "description": "Article FR (élision)"},
    {"rule_type": "article", "pattern": "un",   "replacement": "", "description": "Article FR"},
    {"rule_type": "article", "pattern": "une",  "replacement": "", "description": "Article FR"},
    {"rule_type": "article", "pattern": "des",  "replacement": "", "description": "Article FR"},

    # Release tags (stripped before matching)
    {"rule_type": "release_tag", "pattern": "DIRFIX",      "replacement": "", "description": "Fix tag"},
    {"rule_type": "release_tag", "pattern": "PROPER",      "replacement": "", "description": "Proper release"},
    {"rule_type": "release_tag", "pattern": "PROPER2",     "replacement": "", "description": "Second proper release"},
    {"rule_type": "release_tag", "pattern": "REPACK",      "replacement": "", "description": "Repack release"},
    {"rule_type": "release_tag", "pattern": "RERIP",       "replacement": "", "description": "Re-rip release"},
    {"rule_type": "release_tag", "pattern": "REAL",        "replacement": "", "description": "Real release"},
    {"rule_type": "release_tag", "pattern": "INTERNAL",    "replacement": "", "description": "Internal release"},
    {"rule_type": "release_tag", "pattern": "LIMITED",     "replacement": "", "description": "Limited release"},
    {"rule_type": "release_tag", "pattern": "UNRATED",     "replacement": "", "description": "Unrated version"},
    {"rule_type": "release_tag", "pattern": "EXTENDED",    "replacement": "", "description": "Extended cut"},
    {"rule_type": "release_tag", "pattern": "REMUX",       "replacement": "", "description": "Remux"},
    {"rule_type": "release_tag", "pattern": "COMPLETE",    "replacement": "", "description": "Complete series"},
    {"rule_type": "release_tag", "pattern": "COMPLET",     "replacement": "", "description": "Complete (FR)"},
    {"rule_type": "release_tag", "pattern": "INTEGRALE",   "replacement": "", "description": "Intégrale"},
    {"rule_type": "release_tag", "pattern": "INTEGRAL",    "replacement": "", "description": "Integral"},
    {"rule_type": "release_tag", "pattern": "READNFO",     "replacement": "", "description": "Read NFO tag"},
    {"rule_type": "release_tag", "pattern": "SUBFORCED",   "replacement": "", "description": "Forced subtitles"},
    {"rule_type": "release_tag", "pattern": "DUBBED",      "replacement": "", "description": "Dubbed version"},
    {"rule_type": "release_tag", "pattern": "MULTI",       "replacement": "", "description": "Multilingual"},
    {"rule_type": "release_tag", "pattern": "VOSTFR",      "replacement": "", "description": "VOSTFR"},
    {"rule_type": "release_tag", "pattern": "TRUEFRENCH",  "replacement": "", "description": "True French audio"},
    {"rule_type": "release_tag", "pattern": "FRENCH",      "replacement": "", "description": "French audio"},
    {"rule_type": "release_tag", "pattern": "VFQ",         "replacement": "", "description": "VFQ audio"},
    {"rule_type": "release_tag", "pattern": "VFF",         "replacement": "", "description": "VFF audio"},
    {"rule_type": "release_tag", "pattern": "VF2",         "replacement": "", "description": "VF2 audio"},
    {"rule_type": "release_tag", "pattern": "VOF",         "replacement": "", "description": "VOF audio"},
    {"rule_type": "release_tag", "pattern": "VFI",         "replacement": "", "description": "VFI audio"},
    # Streaming platform tags
    {"rule_type": "release_tag", "pattern": "NF",          "replacement": "", "description": "Netflix"},
    {"rule_type": "release_tag", "pattern": "AMZN",        "replacement": "", "description": "Amazon Prime"},
    {"rule_type": "release_tag", "pattern": "DSNP",        "replacement": "", "description": "Disney+"},
    {"rule_type": "release_tag", "pattern": "ATVP",        "replacement": "", "description": "Apple TV+"},
    {"rule_type": "release_tag", "pattern": "HULU",        "replacement": "", "description": "Hulu"},
    {"rule_type": "release_tag", "pattern": "PCOK",        "replacement": "", "description": "Peacock"},
    {"rule_type": "release_tag", "pattern": "CRKL",        "replacement": "", "description": "Crackle"},
    {"rule_type": "release_tag", "pattern": "STAN",        "replacement": "", "description": "Stan"},
    {"rule_type": "release_tag", "pattern": "BCORE",       "replacement": "", "description": "Bravia Core"},
    {"rule_type": "release_tag", "pattern": "STV",         "replacement": "", "description": "STV"},
    {"rule_type": "release_tag", "pattern": "MAX",         "replacement": "", "description": "Max"},
    {"rule_type": "release_tag", "pattern": "PMTP",        "replacement": "", "description": "Paramount+"},
    # Source/format tags
    {"rule_type": "release_tag", "pattern": "WEBDL",       "replacement": "", "description": "WEB-DL"},
    {"rule_type": "release_tag", "pattern": "WEBRIP",      "replacement": "", "description": "WEBRip"},
    {"rule_type": "release_tag", "pattern": "BLURAY",      "replacement": "", "description": "Blu-Ray"},
    {"rule_type": "release_tag", "pattern": "DVDRIP",      "replacement": "", "description": "DVD Rip"},
    {"rule_type": "release_tag", "pattern": "HDTV",        "replacement": "", "description": "HDTV"},
]

# ── Language rules ────────────────────────────────────────────────────────────

DEFAULT_LANGUAGE_RULES: list[dict] = [
    # French detection patterns (from FRENCH_PATTERNS constant)
    {"rule_type": "french_pattern", "key": "VFF",    "value": r"\b(?:VFF|TRUEFRENCH)\b",  "description": "VFF / True French"},
    {"rule_type": "french_pattern", "key": "VF2",    "value": r"\b(?:VF2)\b",             "description": "VF2"},
    {"rule_type": "french_pattern", "key": "VFQ",    "value": r"\b(?:VFQ)\b",             "description": "VFQ"},
    {"rule_type": "french_pattern", "key": "VFI",    "value": r"\b(?:VFI)\b",             "description": "VFI"},
    {"rule_type": "french_pattern", "key": "VOF",    "value": r"\b(?:VOF)\b",             "description": "VOF"},
    {"rule_type": "french_pattern", "key": "VQ",     "value": r"\b(?:VOQ|VQ)\b",          "description": "VQ / VOQ"},
    {"rule_type": "french_pattern", "key": "VOSTFR", "value": r"\b(?:VOSTFR|SUBFRENCH)\b","description": "VOSTFR / SUBFRENCH"},
    {"rule_type": "french_pattern", "key": "FRENCH", "value": r"\b(?:FRENCH|FR)\b",       "description": "FRENCH / FR"},
    {"rule_type": "french_pattern", "key": "MULTI",  "value": r"\b(?:MULTI)\b",           "description": "MULTI"},

    # Language code mappings (from lang_mapping in language_priority_filter.py)
    {"rule_type": "code_mapping", "key": "fr",      "value": "FRENCH",  "description": "fr → FRENCH"},
    {"rule_type": "code_mapping", "key": "vff",     "value": "VFF",     "description": "vff → VFF"},
    {"rule_type": "code_mapping", "key": "vf",      "value": "FRENCH",  "description": "vf → FRENCH"},
    {"rule_type": "code_mapping", "key": "vostfr",  "value": "VOSTFR",  "description": "vostfr → VOSTFR"},
    {"rule_type": "code_mapping", "key": "multi",   "value": "VFF",     "description": "multi → VFF (priority)"},
    {"rule_type": "code_mapping", "key": "voi",     "value": "VOF",     "description": "voi → VOF"},
    {"rule_type": "code_mapping", "key": "vfi",     "value": "VFI",     "description": "vfi → VFI"},
    {"rule_type": "code_mapping", "key": "vf2",     "value": "VF2",     "description": "vf2 → VF2"},
    {"rule_type": "code_mapping", "key": "vfq",     "value": "VFQ",     "description": "vfq → VFQ"},

    # Priority groups — default mode (vfq_mode = False)
    {"rule_type": "priority_group", "key": "VFF",    "value": "1", "extra": {"vfq_mode": False}, "description": "Groupe 1 (défaut)"},
    {"rule_type": "priority_group", "key": "VOF",    "value": "1", "extra": {"vfq_mode": False}, "description": "Groupe 1 (défaut)"},
    {"rule_type": "priority_group", "key": "VFI",    "value": "1", "extra": {"vfq_mode": False}, "description": "Groupe 1 (défaut)"},
    {"rule_type": "priority_group", "key": "MULTI",  "value": "1", "extra": {"vfq_mode": False}, "description": "Groupe 1 (défaut)"},
    {"rule_type": "priority_group", "key": "VF2",    "value": "2", "extra": {"vfq_mode": False}, "description": "Groupe 2 (défaut)"},
    {"rule_type": "priority_group", "key": "VFQ",    "value": "2", "extra": {"vfq_mode": False}, "description": "Groupe 2 (défaut)"},
    {"rule_type": "priority_group", "key": "VQ",     "value": "2", "extra": {"vfq_mode": False}, "description": "Groupe 2 (défaut)"},
    {"rule_type": "priority_group", "key": "FRENCH", "value": "2", "extra": {"vfq_mode": False}, "description": "Groupe 2 (défaut)"},
    {"rule_type": "priority_group", "key": "VOSTFR", "value": "3", "extra": {"vfq_mode": False}, "description": "Groupe 3 (défaut)"},

    # Priority groups — VFQ mode (vfq_mode = True)
    {"rule_type": "priority_group", "key": "VFQ",    "value": "1", "extra": {"vfq_mode": True}, "description": "Groupe 1 (VFQ)"},
    {"rule_type": "priority_group", "key": "VF2",    "value": "1", "extra": {"vfq_mode": True}, "description": "Groupe 1 (VFQ)"},
    {"rule_type": "priority_group", "key": "VQ",     "value": "1", "extra": {"vfq_mode": True}, "description": "Groupe 1 (VFQ)"},
    {"rule_type": "priority_group", "key": "VFF",    "value": "2", "extra": {"vfq_mode": True}, "description": "Groupe 2 (VFQ)"},
    {"rule_type": "priority_group", "key": "VOF",    "value": "2", "extra": {"vfq_mode": True}, "description": "Groupe 2 (VFQ)"},
    {"rule_type": "priority_group", "key": "VFI",    "value": "2", "extra": {"vfq_mode": True}, "description": "Groupe 2 (VFQ)"},
    {"rule_type": "priority_group", "key": "FRENCH", "value": "2", "extra": {"vfq_mode": True}, "description": "Groupe 2 (VFQ)"},
    {"rule_type": "priority_group", "key": "MULTI",  "value": "2", "extra": {"vfq_mode": True}, "description": "Groupe 2 (VFQ)"},
    {"rule_type": "priority_group", "key": "VOSTFR", "value": "3", "extra": {"vfq_mode": True}, "description": "Groupe 3 (VFQ)"},

    # FR release groups (from FR_RELEASE_GROUPS constant)
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 01",
        "value": r"(?<=[.\s\-\[])(BlackAngel|Choco|Sicario|Tezcat74|TyrellCorp|Zapax)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 1",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 02",
        "value": r"(?<=[.\s\-\[])(FtLi|Goldenyann|MUSTANG|Obi|PEPiTE|QUEBEC63|QC63|ROMKENT|R3MiX)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 2",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 03",
        "value": r"(?<=[.\s\-\[])(FLOP|FRATERNiTY|QTZ|PopHD|toto70300|GHT|EXTREME|AvALoN|KFL|mHDgz)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 3",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 04",
        "value": r"(?<=[.\s\-\[])(DUSTiN|QUALiTY|Tsundere-Raws|LAZARUS|ALFA|SODAPOP|Tetine|DREAM|Winks)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 4",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 05",
        "value": r"(?<=[.\s\-\[])(BDHD|MAX|SowHD|SN2P|RG|BTT|KAF|AwA|MULTiViSiON|FERVEX|Foxhound|K7|LiBERTAD)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 5",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 06",
        "value": r"(?<=[.\s\-\[])(FUJiSAN|HDForever|MARBLECAKE|MYSTERiON|ONLY|UTT|ZiT|JP48|SEL|PATOMiEL)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 6",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 07",
        "value": r"(?<=[.\s\-\[])(BONBON|FCK|FW|FoX|FrIeNdS|MOONLY|MTDK|PATOPESTO|Psaro|T3KASHi|TFA)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 7",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 08",
        "value": r"(?<=[.\s\-\[])(ALLDAYiN|ARK01|HANAMi|HeavyWeight|NEO|NoNe|ONLYMOViE|Slay3R|TkHD)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 8",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 09",
        "value": r"(?<=[.\s\-\[])(4FR|AiR3D|AiRDOCS|AiRFORCE|AiRLiNE|AiRTV|AKLHD|AMB3R|SERQPH|Elcrackito)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 9",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 10",
        "value": r"(?<=[.\s\-\[])(ANMWR|AVON|AYMO|AZR|BANKAi|BAWLS|BiPOLAR|BLACKPANTERS|BODIE|BOOLZ|BRiNK|CARAPiLS|CiELOS)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 10",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 11",
        "value": r"(?<=[.\s\-\[])(CiNEMA|CMBHD|CoRa|COUAC|CRYPT0|D4KiD|DEAL|DiEBEX|DUPLI|DUSS|ENJOi|EUBDS|FHD|FiDELiO|FiDO|ForceBleue)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 11",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 12",
        "value": r"(?<=[.\s\-\[])(FREAMON|FRENCHDEADPOOL2|FRiES|FUTiL|FWDHD|GHOULS|GiMBAP|GLiMMER|Goatlove|HERC|HiggsBoson|HiRoSHiMa)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 12",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 13",
        "value": r"(?<=[.\s\-\[])(HYBRiS|HyDe|JMT|JoKeR|JUSTICELEAGUE|KAZETV|L0SERNiGHT|LaoZi|LeON|LOFiDEL|LOST|LOWIMDB|LYPSG|MAGiCAL)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 13",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 14",
        "value": r"(?<=[.\s\-\[])(MANGACiTY|MAXAGAZ|MaxiBeNoul|McNULTY|MELBA|MiND|MORELAND|MUNSTER|MUxHD|NERDHD|NERO|NrZ|NTK|OBSTACLE)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 14",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 15",
        "value": r"(?<=[.\s\-\[])(OohLaLa|OOKAMI|PANZeR|PiNKPANTERS|PKPTRS|PRiDEHD|PROPJOE|PURE|PUREWASTEOFBW|ROUGH|RUDE|Ryotox|SAFETY)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 15",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 16",
        "value": r"(?<=[.\s\-\[])(SASHiMi|SEiGHT|SESKAPiLE|SHEEEiT|SHiNiGAMi(?:UHD)?|SiGeRiS|SILVIODANTE|SLEEPINGFOREST|SODAPOP|S4LVE|SPINE)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 16",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 17",
        "value": r"(?<=[.\s\-\[])(SPOiLER|STRINGERBELL|SUNRiSE|tFR|THENiGHTMAREiNHD|THiNK|THREESOME|TiMELiNE|TSuNaMi|UKDHD|UKDTV|ULSHD|Ulysse)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 17",
    },
    {
        "rule_type": "release_group",
        "key": "FR Release Groups 18",
        "value": r"(?<=[.\s\-\[])(USUNSKiLLED|URY|VENUE|VFC|VoMiT|Wednesday29th|ZEST|ZiRCON)(?=[.\s\-\]]|$)",
        "description": "Groupes de release FR — lot 18",
    },
]


async def seed_if_empty(db_session_factory) -> None:
    """
    Seed title_normalization_rules and language_rules tables if they are empty.
    Called at app startup after DB tables are created.
    """
    from stream_fusion.services.postgresql.dao.title_normalization_rule_dao import TitleNormalizationRuleDAO
    from stream_fusion.services.postgresql.dao.language_rule_dao import LanguageRuleDAO
    from stream_fusion.logging_config import logger

    async with db_session_factory() as session:
        title_dao = TitleNormalizationRuleDAO(session)
        lang_dao = LanguageRuleDAO(session)

        title_count = await title_dao.count()
        if title_count == 0:
            await title_dao.bulk_create(DEFAULT_TITLE_RULES)
            logger.info(f"Seeds: inserted {len(DEFAULT_TITLE_RULES)} default title normalization rules")
        else:
            logger.debug(f"Seeds: title_normalization_rules already has {title_count} rows, skipping")

        lang_count = await lang_dao.count()
        if lang_count == 0:
            await lang_dao.bulk_create(DEFAULT_LANGUAGE_RULES)
            logger.info(f"Seeds: inserted {len(DEFAULT_LANGUAGE_RULES)} default language rules")
        else:
            logger.debug(f"Seeds: language_rules already has {lang_count} rows, skipping")
