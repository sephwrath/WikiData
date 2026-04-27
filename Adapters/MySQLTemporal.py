
from WikiData.Domain.DataClasses import DumpFile, Article, ArticleSection, ArticleSectionLink, DumpProgress


class MySQLTemporalAdapter:

    def __init__(self, host: str, user: str, password: str, database: str, pool_name: str, pool_size: int):
        dbconfig = {
            "database": database,
            "user": user,
            "password": password,
            "host": host
        }
        self.cnxPool : pooling.MySQLConnectionPool = pooling.MySQLConnectionPool(pool_name=pool_name, pool_size=pool_size, **dbconfig)
        

    def commit(self, connection):
        if connection.is_connected():
            connection.commit()

    def get_dump_file_by_name(cursor, file_name: str) -> DumpFile | None:
        sql = """
            SELECT id, file_name, tar_info, offset, offset_data
            FROM dump_file
            WHERE file_name = %s
        """
        cursor.execute(sql, (file_name,))
        row = cursor.fetchone()

        if not row:
            return None

        return DumpFile(
            id=row[0],
            file_name=row[1],
            tar_info=row[2],
            offset=row[3],
            offset_data=row[4],
        )

    def insert_dump_file(
        cursor,
        file_name: str,
        tar_info: bytes,
        offset: int,
        offset_data: int
    ) -> DumpFile:
        sql = """
            INSERT INTO dump_file (file_name, tar_info, offset, offset_data)
            VALUES (%s, _binary %s, %s, %s)
        """
        cursor.execute(sql, (file_name, tar_info, offset, offset_data))

        new_id = cursor.lastrowid

        return DumpFile(
            id=new_id,
            file_name=file_name,
            tar_info=tar_info,
            offset=offset,
            offset_data=offset_data,
        )

    def update_dump_file(
        cursor,
        dump_file_id: int,
        tar_info: bytes,
        offset: int,
        offset_data: int
    ) -> DumpFile:
        sql = """
            UPDATE dump_file
            SET tar_info = _binary %s,
                offset = %s,
                offset_data = %s
            WHERE id = %s
        """
        cursor.execute(sql, (tar_info, offset, offset_data, dump_file_id))

        return DumpFile(
            id=dump_file_id,
            file_name="",  # unknown here unless re-fetched
            tar_info=tar_info,
            offset=offset,
            offset_data=offset_data,
        )

    def upsert_dump_file(cursor, file_name: str, tar_info: bytes, offset: int, offset_data: int) -> DumpFile:
        existing = self.get_dump_file_by_name(cursor, file_name)

        if existing:
            return update_dump_file(
                cursor,
                existing.id,
                tar_info,
                offset,
                offset_data
            )
        else:
            return insert_dump_file(
                cursor,
                file_name,
                tar_info,
                offset,
                offset_data
            )
        
    def get_last_dump_progress(cursor) -> DumpProgress:
        sql = """
            SELECT MAX(dump_file_id) AS df_id,
                MAX(dump_idx) AS df_idx
            FROM article
            WHERE dump_file_id = (
                SELECT MAX(dump_file_id) FROM article
            )
        """
        cursor.execute(sql)
        row = cursor.fetchone()

        if not row or row[0] is None:
            return DumpProgress(dump_file_id=None, dump_index=None)

        return DumpProgress(
            dump_file_id=row[0],
            dump_index=row[1],
        )

    from typing import List

    def get_dump_files_from_id(cursor, start_id: int) -> List[DumpFile]:
        sql = """
            SELECT id, file_name, tar_info, offset, offset_data
            FROM dump_file
            WHERE id >= %s
            ORDER BY id
        """
        cursor.execute(sql, (start_id,))
        rows = cursor.fetchall()

        return [
            DumpFile(
                id=row[0],
                file_name=row[1],
                tar_info=row[2],
                offset=row[3],
                offset_data=row[4],
            )
            for row in rows
        ]
    
    def save_article(self, cursor, article: Article):
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
        cursor.execute(sql, (
            article.id,
            article.title,
            article.title_srch,
            article.description,
            article.file_update,
            article.dump_file_id,
            article.dump_idx,
            article.url,
            article.redirect,
            article.no_dates,
            article.wiki_update_ts,
            article.err
        ))