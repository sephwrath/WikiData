from dataclasses import fields
from typing import List
import psycopg
#from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from WikiData.Domain.TemporalDBPort import TemporalDBPort
from WikiData.Domain.DataClasses import (
    ArticleSectionFormat, DumpFile, Article,
    ArticleSection, ParsedEvent, DumpProgress, Dump
)


class PostgresTemporalAdapter(TemporalDBPort):

    def __init__(self, dsn: str, pool_size: int = 5):
        self.pool = ConnectionPool(conninfo=dsn, max_size=pool_size)
        self.connection = None
        self.cursor = None

    def start(self):
        self.connection = self.pool.getconn()
        self.cursor = self.connection.cursor()

    def end(self):
        try:
            self.connection.commit()
        finally:
            self.cursor.close()
            self.pool.putconn(self.connection)

    def commit(self):
        self.connection.commit()


    # ------------------------
    # Utility
    # ------------------------
    @staticmethod
    def map_row_to_dataclass(cls, row: dict):
        field_names = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in row.items() if k in field_names})
    
    # ------------------------
    # Dump
    # ------------------------
    def create_new_dump(self, Dump: Dump) -> DumpFile:
        sql = """
            INSERT INTO dump (file_name, file_path, extract_date, current)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        self.cursor.execute(sql, (Dump.file_name, Dump.file_path, Dump.date, Dump.current))
        new_id = self.cursor.fetchone()["id"]

        return DumpFile(
            id=new_id,
            dump_id=new_id,
            file_name=Dump.file_name,
            tar_info=b'',
            offset=0,
            offset_data=0
        )
    
    def set_current_dump(self, dump_file_id: int) -> None:
        sql_unset_current = "UPDATE dump SET current = FALSE WHERE current = TRUE"
        sql_set_current = "UPDATE dump SET current = TRUE WHERE id = %s"

        self.cursor.execute(sql_unset_current)
        self.cursor.execute(sql_set_current, (dump_file_id,))

    def get_current_dump(self) -> Dump | None:
        sql = "SELECT id, file_name, file_path, extract_date, current FROM dump WHERE current = TRUE"
        self.cursor.execute(sql)
        row = self.cursor.fetchone()

        if not row:
            return None

        return Dump(
            id=row["id"],
            file_name=row["file_name"],
            file_path=row["file_path"],
            date=row["extract_date"],
            current=row["current"]
        )

    # ------------------------
    # Dump File
    # ------------------------
    def get_dump_file_by_name(self, file_name: str) -> DumpFile | None:
        sql = """
            SELECT id, file_name, tar_info, "offset", offset_data
            FROM dump_file
            WHERE file_name = %s
        """
        self.cursor.execute(sql, (file_name,))
        row = self.cursor.fetchone()
        return DumpFile(**row) if row else None

    def get_dump_files_from_id(self, start_id: int) -> List[DumpFile]:
        sql = """
            SELECT id, file_name, tar_info, "offset", offset_data
            FROM dump_file
            WHERE id >= %s
            ORDER BY id
        """
        self.cursor.execute(sql, (start_id,))
        return [DumpFile(**row) for row in self.cursor.fetchall()]

    def insert_dump_file(self, dump_file: DumpFile) -> DumpFile:
        sql = """
            INSERT INTO dump_file (file_name, tar_info, "offset", offset_data)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """
        self.cursor.execute(sql, (dump_file.file_name, dump_file.tar_info, dump_file.offset, dump_file.offset_data))
        new_id = self.cursor.fetchone()["id"]

        return dump_file._replace(id=new_id)
    
    def update_dump_file(self, dump_file_id, tar_info, offset, offset_data) -> DumpFile:
        sql = """
            UPDATE dump_file
            SET tar_info = %s,
                "offset" = %s,
                offset_data = %s
            WHERE id = %s
        """
        self.cursor.execute(sql, (tar_info, offset, offset_data, dump_file_id))

        return DumpFile(
            id=dump_file_id,
            file_name="",
            tar_info=tar_info,
            offset=offset,
            offset_data=offset_data,
        )

    # ------------------------
    # Dump progress
    # ------------------------
    def get_last_dump_progress(self) -> DumpProgress:
        sql = """
            SELECT MAX(dump_file_id) AS df_id,
                   MAX(dump_idx) AS df_idx
            FROM article
            WHERE dump_file_id = (
                SELECT MAX(dump_file_id) FROM article
            )
        """
        self.cursor.execute(sql)
        row = self.cursor.fetchone()

        if not row or row["df_id"] is None:
            return DumpProgress(dump_file_id=None, dump_index=None)

        return DumpProgress(
            dump_file_id=row["df_id"],
            dump_index=row["df_idx"]
        )

    # ------------------------
    # Article
    # ------------------------
    def save_article(self, article: Article):
        sql = """
            INSERT INTO article (
                id, title, title_srch, description, file_update,
                dump_file_id, dump_idx, url, redirect,
                no_dates, wiki_update_ts, err
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                title_srch = EXCLUDED.title_srch,
                description = EXCLUDED.description,
                file_update = EXCLUDED.file_update,
                dump_file_id = EXCLUDED.dump_file_id,
                dump_idx = EXCLUDED.dump_idx,
                url = EXCLUDED.url,
                redirect = EXCLUDED.redirect,
                no_dates = EXCLUDED.no_dates,
                wiki_update_ts = EXCLUDED.wiki_update_ts,
                err = EXCLUDED.err
        """
        self.cursor.execute(sql, (
            article.id, article.title, article.title_srch,
            article.description, article.update,
            article.dump_file_id, article.dump_idx,
            article.url, article.redirect,
            article.no_dates, article.wiki_update_ts, article.err
        ))

    # ------------------------
    # Sections (example)
    # ------------------------
    def get_article_sections(self, article_id: int) -> List[ArticleSection]:
        sql = "SELECT * FROM article_section WHERE article_id = %s"
        self.cursor.execute(sql, (article_id,))
        return [ArticleSection(**row) for row in self.cursor.fetchall()]

    # ------------------------
    # Events
    # ------------------------
    def save_article_section_events(self, parsed_events: List[ParsedEvent]):
        sql = """
            INSERT INTO parsed_event (
                article_id, section_id, start_date, end_date,
                date_text, start_pos, end_pos, display_text
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.executemany(sql, [
            (
                pe.article_id, pe.section_id,
                pe.start_date, pe.end_date,
                pe.date_text, pe.start_pos,
                pe.end_pos, pe.display_text
            )
            for pe in parsed_events
        ])

    def get_article_by_id(self, article_id: int) -> Article | None:
        select_article = """select a.id, title, `update`, dump_idx, url, redirect, no_dates, wiki_update_ts, err, 
        df.file_name, df.tar_info, df.offset, df.offset_data
        from article a, dump_file df
        where dump_file_id = df.id 
        and a.id = %s"""
        
        self.cursor.execute(select_article, (article_id,))
        row = self.cursor.fetchone()

        if not row:
            return None

        return Article(
            id=row[0],
            title=row[1],
            update=row[2],
            dump_idx=row[3],
            url=row[4],
            redirect=row[5],
            no_dates=row[6],
            wiki_update_ts=row[7],
            err=row[8]
        )

    def clear_article_sections(self, article_id: int):
        delete_article_section = "delete from article_section where article_id = %s"
        delete_article_section_ext_text = "delete from article_section_ext_text where article_id = %s"
        delete_article_section_format = "delete from article_section_format where article_id = %s"
        delete_parsed_event = "delete from parsed_event where article_id = %s"
        
        self.cursor.execute(delete_article_section, (article_id,))
        self.cursor.execute(delete_article_section_ext_text, (article_id,))
        self.cursor.execute(delete_article_section_format, (article_id,))
        self.cursor.execute(delete_parsed_event, (article_id,))

    def save_article(self, article: Article):
        sql = """
            INSERT INTO article (id, title, title_srch, description, file_update, dump_file_id, dump_idx, url, redirect, no_dates, wiki_update_ts, err)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                title_srch = VALUES(title_srch),
                description = VALUES(description),
                file_update = VALUES(file_update),
                dump_file_id = VALUES(dump_file_id),
                dump_idx = VALUES(dump_idx),
                url = VALUES(url),
                redirect = VALUES(redirect),
                no_dates = VALUES(no_dates),
                wiki_update_ts = VALUES(wiki_update_ts),
                err = VALUES(err)
        """
        self.cursor.execute(sql, (article.id, article.title, article.title_srch, article.description,
            article.update, article.dump_file_id, article.dump_idx, article.url, article.redirect,
            article.no_dates, article.wiki_update_ts, article.err))
        
    def update_article(self, article: Article):
        sql = """
            UPDATE article
            SET title = %s,
                title_srch = %s,
                description = %s,
                file_update = %s,
                dump_file_id = %s,
                dump_idx = %s,
                url = %s,
                redirect = %s,
                no_dates = %s,
                wiki_update_ts = %s,
                err = %s
            WHERE id = %s
        """
        self.cursor.execute(sql, (article.title, article.title_srch, article.description,
            article.update, article.dump_file_id, article.dump_idx, article.url, article.redirect,
            article.no_dates, article.wiki_update_ts, article.err, article.id))
        
    def get_article_sections(self, article_id: int) -> List[ArticleSection]:
        select_article_section = "select * from article_section where article_id = %s"
        select_article_section_ext_text = "select * from article_section_ext_text where article_id = %s"

        self.cursor.execute(select_article_section, (article_id,))
        remaining_sections = self.cursor.fetchall()
        ext_text = self.cursor.execute(select_article_section_ext_text, (article_id,))
        remaining_ext_text = self.cursor.fetchall()
        for ext_text in remaining_ext_text:
            section = next(filter(lambda s: s['section_id'] == ext_text['section_id'], remaining_sections), None)
            if section is not None:
                section.text = section['text'] + ext_text['text']

        return [
            ArticleSection(
                article_id=row[0],
                section_id=row[1],
                tag=row[2],
                ext_text_count=row[3],
                parent_section_id=row[4],
                row_idx=row[5],
                column_idx=row[6],
                row_span=row[7],
                column_span=row[8],
                format=row[9],
                text=row[10]
            )
            for row in remaining_sections
        ]
    
    def get_article_sections_unparsed(self, article_id: int) -> List[ArticleSection]:
        # TODO
        select_article_section = "select article_id, section_id, text from article_section where article_id = %s and is_parsed is null"
        select_article_section_ext_text = """SELECT aset.article_id as article_id, aset.section_id, aset.count_id, aset.text
            FROM article_section_ext_text aset
            inner join article_section asect on aset.article_id = asect.article_id and aset.section_id = asect.section_id
            where asect.is_parsed is null and aset.article_id = %s"""
        
        self.cursor.execute(select_article_section, (article_id,))
        remaining_sections = self.cursor.fetchall()
        ext_text = self.cursor.execute(select_article_section_ext_text, (article_id,))
        remaining_ext_text = self.cursor.fetchall()
        for ext_text in remaining_ext_text:
            section = next(filter(lambda s: s['section_id'] == ext_text['section_id'], remaining_sections), None)
            if section is not None:
                section.text = section['text'] + ext_text['text']

        return [
            ArticleSection(
                article_id=row[0],
                section_id=row[1],
                text=row[10]
            )
            for row in remaining_sections
        ]

    def save_article_section(self, article_section: ArticleSection):
        insert_article_section = """INSERT INTO article_section (article_id, section_id, tag, ext_text_count, parent_section_id, 
        row_idx, column_idx, row_span, column_span, format, text) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        section_text = article_section.text

        self.cursor.execute(insert_article_section, (article_section.article_id, article_section.section_id, article_section.tag, 
                                                 None, article_section.parent_section_id,
                                                  article_section.row_idx, article_section.column_idx, article_section.row_span,
                                                  article_section.column_span, article_section.format, section_text))        

    def save_article_section_ext_text(self, article_id: int, section_id: int, ext_text_chunks: List[str]):
        insert_article_section_ext_text = "INSERT INTO article_section_ext_text (article_id, section_id, count_id, text) values (%s, %s, %s, %s)"

        for (chunk_idx, section_chunk) in enumerate(ext_text_chunks[1:]):
            self.cursor.execute(insert_article_section_ext_text, (article_id, section_id, chunk_idx, section_chunk))


    def save_article_section_formats(self, article_section_formats: List[ArticleSectionFormat]):
        insert_article_section_format = """INSERT INTO article_section_format (article_id, section_id, format, start_pos, end_pos, link)
            VALUES (%s, %s, %s, %s, %s, %s)"""
        self.cursor.executemany(insert_article_section_format, [
            (asf.article_id, asf.section_id, asf.format, asf.start_pos, asf.end_pos, asf.link)
            for asf in article_section_formats
        ])


    def get_article_section_formats(self, article_id: int) -> List[ArticleSectionFormat]:
        select_article_section_format = "select * from article_section_format where article_id = %s"
        self.cursor.execute(select_article_section_format, (article_id,))
        rows = self.cursor.fetchall()

        return [
            ArticleSectionFormat(
                article_id=row[0],
                section_id=row[1],
                format=row[2],
                start_pos=row[3],
                end_pos=row[4],
                link=row[5]
            )
            for row in rows
        ]


    def get_article_section_events(self, article_id: int) -> List[ParsedEvent]:
        select_parsed_event = "select * from parsed_event where article_id = %s"
        self.cursor.execute(select_parsed_event, (article_id,))
        rows = self.cursor.fetchall()
        return [
            ParsedEvent(
                id=row[0],
                article_id=row[1],
                section_id=row[2],
                start_date=row[3],
                end_date=row[4],
                parse_status=row[5],
                date_text=row[6]
            )
            for row in rows
        ]
    
    def save_article_section_events(self, parsed_events: List[ParsedEvent]):
        insert_parsed_event = """INSERT INTO parsed_event (article_id, section_id, 
            start_date, end_date, date_text, start_pos, end_pos, display_text) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        self.cursor.executemany(insert_parsed_event, [
            (pe.article_id, pe.section_id, pe.start_date, pe.end_date, pe.date_text, pe.start_pos, pe.end_pos, pe.display_text)
            for pe in parsed_events
        ])


    def update_article_section_parse_status(self, article_id: int, section_id: int, is_parsed: bool):
        update_section = "UPDATE article_section SET is_parsed = 'Y' WHERE article_id = %s and section_id = %s"
        self.cursor.execute(update_section, (article_id, section_id))

    def get_missing_article_sections(self, article_id: int, last_section_id: int):
        select_article_section = """select article_id, section_id from article_section 
        where article_id = %s and section_id > %s and is_parsed is not null"""
        self.cursor.execute(select_article_section, (article_id, last_section_id))
        return self.cursor.fetchall()

    def article_search(self, search: str, limit: int = 10):
        search_articles= """(select COALESCE(rdr.id, art.id) AS id,
                    COALESCE(rdr.title, art.title) AS title,
                    COALESCE(rdr.description, art.description) AS description
                from article as art
                    left outer join article as rdr  on art.redirect = rdr.title
                where art.title like %s limit %s)
                union
                (select id, title, description from (
                    select distinct COALESCE(rdr.id, art.id) AS id,
                        COALESCE(rdr.title, art.title) AS title,
                        COALESCE(rdr.description, art.description) AS description,
                        MATCH(art.title_srch) AGAINST (%s IN NATURAL LANGUAGE MODE) AS score
                    from article as art
                    left outer join article as rdr  on art.redirect = rdr.title
                    WHERE MATCH(art.title_srch) AGAINST (%s IN NATURAL LANGUAGE MODE)
                    ORDER BY score DESC
                    limit %s) as st);"""
        self.cursor.execute(search_articles, (search + '%', limit, search, search, limit))
        results = self.cursor.fetchall()
        return results

