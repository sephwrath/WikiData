from dataclasses import dataclass
from typing import Optional
from datetime import datetime


# -------------------------
# dump_file
# -------------------------
@dataclass
class DumpFile:
    id: int
    file_name: str
    tar_info: bytes
    offset: int
    offset_data: int

@dataclass
class DumpProgress:
    dump_file_id: Optional[int]
    dump_index: Optional[int]




# -------------------------
# article
# -------------------------
@dataclass
class Article:
    id: int
    title: str
    title_srch: Optional[str]
    description: Optional[str]
    update: datetime
    dump_file_id: Optional[int]
    dump_idx: Optional[int]
    url: str
    redirect: Optional[str]
    no_dates: Optional[bool]
    wiki_update_ts: Optional[datetime]
    err: Optional[str]


# -------------------------
# article_section
# -------------------------
@dataclass
class ArticleSection:
    article_id: int
    section_id: int
    ext_text_count: Optional[int]
    parent_section_id: Optional[int]
    row_idx: Optional[int]
    column_idx: Optional[int]
    row_span: Optional[int]
    column_span: Optional[int]
    tag: str
    format: Optional[str]
    text: Optional[str]
    is_parsed: Optional[str]  # char(1)




# -------------------------
# article_section_format
# -------------------------
@dataclass
class ArticleSectionFormat:
    id: int
    article_id: int
    section_id: int
    start_pos: int
    end_pos: int
    format: str
    link: Optional[str]


# -------------------------
# parsed_event
# -------------------------
@dataclass
class ParsedEvent:
    id: int
    article_id: int
    section_id: int
    start_date: Optional[int]
    end_date: Optional[int]
    parse_status: Optional[int]
    date_text: Optional[str]
    start_pos: int
    end_pos: int
    display_text: Optional[str]