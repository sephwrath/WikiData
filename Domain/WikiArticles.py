from typing import Tuple, List
import tarfile
import os
import json
import re
import datetime as dt
import time

from bs4 import BeautifulSoup
from WikiData.Domain.DataClasses import DumpFile, DumpProgress, Article, ArticleSection, ArticleSectionLink, ArticleSectionFormat, ParsedEvent
from WikiData.Domain.Errors import DatabaseError, DumpFileError,  ArticleNotFoundError
from WikiData.Domain.TemporalDBPort import TemporalDBPort
from WikiData.wikiHtmllParse import WikiHtmlParser
from WikiData.Domain.WikiDump import WikiDump

class wikidateExtractor:

    def __init__(self, temporal_adapter : TemporalDBPort, wiki_dump : WikiDump, wikiHtmlParser : WikiHtmlParser):
        self.dbadapter = temporal_adapter
        self.wiki_dump = wiki_dump
        self.wikiHtmlParser = wikiHtmlParser

    def start_time(self):
        # timing variables
        self.t_count = 0
        self.t_total = 0
        self.t_s = 0
        self.t_e = 0
        self.t_s = time.time()
    
    def update_time(self):
        self.t_e = time.time()
        t_tot = self.t_e - self.t_s
        self.t_count += 1
        self.t_total += t_tot
        t_avg = self.t_total / self.t_count
        print("processing time: {}. Total time: {}, average time for {} articles: {}".format(t_tot, self.t_total, self.t_count, t_avg))
        
    def get_missing_dump_details(self) -> Tuple[int, List[DumpFile]]:
        progress = self.dbadapter.get_last_dump_progress()
        # defaults
        dump_index = -1
        dump_file_id = 1

        if progress.dump_file_id is not None:
            dump_index = progress.dump_index or -1
            dump_file_id = progress.dump_file_id

        dump_files = self.dbadapter.get_dump_files_from_id(dump_file_id)

        return dump_index, dump_files

    def extract_file_names(self,file_path: str):
        for member in self.wiki_dump.file_name_generator(file_path):
            self.dbadapter.upsert_dump_file(
                file_name=member.name,
                tar_info=member.tobuf(),
                offset=member.offset,
                offset_data=member.offset_data
            )
            self.dbadapter.commit()

    def get_missing_dump_details(self) -> Tuple[int, List[DumpFile]]:
        progress = self.dbadapter.get_last_dump_progress()

        # defaults
        dump_index = -1
        dump_file_id = 1

        if progress.dump_file_id is not None:
            dump_index = progress.dump_index or -1
            dump_file_id = progress.dump_file_id

        dump_files = self.dbadapter.get_dump_files_from_id(dump_file_id)

        return dump_index, dump_files

    def write_article_lines_to_db(self, line : str, dump_id : int, dump_idx : int):
        self.start_time()

        article = json.loads(line)
        title = article['name']
        url = article["url"]

        no_events = None
        redirect = None
        now_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # easiest way to check for redirects is to see if there is a #REDIRECT in the wikitext
        if "wikitext" in article["article_body"]:
            wikiText = article["article_body"]["wikitext"]
            redirects = re.findall(r'#REDIRECT\[\[(.*)\]\]', wikiText)
            if len(redirects) > 0:
                print("parsing: {}, redirects {}".format(title, redirects))
                redirect = redirects[0]
        
        try:
            mod_date = None
            if 'date_modified' in article:
                mod_date = dt.datetime.strptime(article['date_modified'], '%Y-%m-%dT%H:%M:%SZ')
            description = None 
            if 'abstract' in article:
                if len(article['abstract']) > 1000:
                    description = article['abstract'][:997] + "..."
                else:
                    description = article['abstract']

            article = Article(title, now_time, dump_id, dump_idx, url, redirect, no_events, mod_date, description)
            self.dbadapter.save_article(article=article)
            if 'redirects' in article:
                for redirect in article['redirects']:
                    if 'url' not in redirect:
                        redirect['url'] = None
                    if 'name' in redirect:
                        redirect_article = Article(redirect['name'], now_time, dump_id, dump_idx, redirect['url'], None, no_events, mod_date, None)
                        self.dbadapter.save_article(article=redirect_article)

            self.dbadapter.commit()
        except DatabaseError as err:
            print("Database error: {}".format(err))

        self.update_time()
        print("title: {}".format(title))

    
    def extract_json_article_to_article_tbl(self, json_save_path : str) -> None:
        file_start_offset, dumps = self.get_missing_dump_details()
        # 0 - dump_idx - the last article inserted into the database
        # 1 - dump_files - id, file_name, tar_info, offset, offset_data
        
        first_file_id = dumps[0].id

        # if we are using the json files then just read the file
        if json_save_path is not None:
            # save the json to a file
            for file_rec in dumps:

                if file_rec.id > first_file_id:
                        file_start_offset = -1
                for (idx, line) in self.wiki_dump.file_gnerator(json_save_path + file_rec.file_name, file_start_offset):
                    self.write_article_lines_to_db(line, file_rec.id, idx)
        
        return
    
        # get just the article details for the article_table 
    def extract_tar_article_to_article_tbl(self, file_path : str) -> None:
        file_start_offset, dumps = self.get_missing_dump_details()
        # 0 - dump_idx - the last article inserted into the database
        # 1 - dump_files - id, file_name, tar_info, offset, offset_data
        
        first_file_id = dumps[0].id

        if file_path is not None:
            for (line, idx, file_id) in self.wiki_dump.tar_file_line_generator(file_path, dumps, file_start_offset, first_file_id):
                self.write_article_lines_to_db(line, file_id, idx)
        
        return


    def parse_article(self, article_id : int, line : str):
        
        self.start_time()

        #wikiHtmlParser = WikiHtmlParser()
        raw_html = ""
        article = json.loads(line)

        raw_html = article["article_body"]["html"]
        title = article['name']
        url = article["url"]

        # replace dashes with hyphens - spacy doesn't recognize dashes
        raw_html = raw_html.replace("–", "-")

        no_events = True
        redirect = None
        now_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        parsed_html = BeautifulSoup(raw_html, features="html.parser")
                            
        self.wikiHtmlParser.parse(parsed_html, title)
        #wikiHtmlParser.parseEvents()
        print("parsing: {}, sections {}, events {}, links {}".format(title, 
                                                                     len(self.wikiHtmlParser.saveSections), 
                                                                     len(self.wikiHtmlParser.sectionEvents), 
                                                                     len(self.wikiHtmlParser.sectionFormats)))

        # insert the article sections
        for (sec_idx, section) in enumerate(self.wikiHtmlParser.saveSections):
            #returned table values - column_span, row_span, row, column, type
            if section.type == self.wikiHtmlParser.TYPE_TABLE_CELL or section.type == self.wikiHtmlParser.TYPE_LIST_ITEM:
                # (article_id, section_id, tag, ext_text_count, parent_section_id, row_idx, column_idx, row_span, column_span, format, text)
                article_section = ArticleSection(article_id, sec_idx, None, section.parent_section, section.row, section.column, section.row_span, 
                                                 section.column_span, section.type, section.format, section.text, 'Y')
            else:
                article_section = ArticleSection(article_id, sec_idx, None, section.parent_section, None, None, None, 
                                                 None, section.type, None, section.text, 'Y')
            
            self.dbadapter.save_article_section(article_section)

        # insert the article section links
        section_formats = []
        for link in self.wikiHtmlParser.sectionFormats:
            # (article_id, section_id, start_pos, end_pos, link)                
            section_formats.append(ArticleSectionFormat(article_id, link.section, link.start, link.end,link.format, link.article))
        
        self.dbadapter.save_article_section_formats(section_formats)

        insert_values = Article(article_id,update=now_time, redirect=redirect, no_dates=no_events, err='UP_TO_DATE')
        self.dbadapter.update_article(insert_values)
        self.dbadapter.commit()

        self.update_time(title)

    def extract_article_detail_by_id(self, article_id: int, file_path : str, json_save_path : str = None, force : bool = False):
        # check if the article needs to be updated
        article = self.dbadapter.get_article_by_id(article_id)

        if article is None:
            print("article {} not found".format(article_id))
            return
        
        dump = self.dbadapter.get_dump_file_by_id(article.dump_file_id)

        if article.err != 'UP_TO_DATE' or force:
            # if it does then delete all the sections, rows, cells and links
            self.dbadapter.delete_article_sections(article_id)

            # if we are using the json files then just read the file
            if json_save_path is not None:
                # save the json to a file
                idx, line = self.wiki_dump.json_file_article(json_save_path, dump, article)
                self.parse_article(article_id, line)
            else:
                idx, line = self.wiki_dump.tar_file_article(file_path, dump, article)
                self.parse_article(article_id, line)
        
        article_events = self.dbadapter.get_article_section_events(article_id)
        article_links = self.dbadapter.get_article_section_formats(article_id)
        article_sections = self.dbadapter.get_article_sections(article_id)

        return (article, article_sections, article_links, article_events)

    def extract_remaining_article_sections_by_id(article_id : int, cursor : cursor.MySQLCursor):
        select_article_section = "select article_id, section_id, text from article_section where article_id = %s and is_parsed is null"
        select_article_section_ext_text = """SELECT aset.article_id as article_id, aset.section_id, aset.count_id, aset.text
            FROM article_section_ext_text aset
            inner join article_section asect on aset.article_id = asect.article_id and aset.section_id = asect.section_id
            where asect.is_parsed is null and aset.article_id = %s"""
        
        cursor.execute(select_article_section, (article_id,))
        remaining_sections = cursor.fetchall()
        ext_text = cursor.execute(select_article_section_ext_text, (article_id,))
        remaining_ext_text = cursor.fetchall()
        for ext_text in remaining_ext_text:
            section = next(filter(lambda s: s['section_id'] == ext_text['section_id'], remaining_sections), None)
            if section is not None:
                section.text = section['text'] + ext_text['text']
        return remaining_sections

    def parse_section_events(self,article_id : int, section_id : int, section_text : str):
        # reset the section events before parsing to avoid duplicates if we need to re-parse
        self.wikiHtmlParser.sectionEvents = []
        self.wikiHtmlParser.extract_events_spacy(section_text, section_id)

        parsed_events = []
        for section in self.wikiHtmlParser.sectionEvents:
            # (article_id, section_id, start_date, end_date, date_text, start_pos, end_pos, display_text
            # event = { 'section': idx,  'startPos': startPos, 'endPos': endPos, 'dText': dText, 'desc': desc }
            parsed_events.append(ParsedEvent(article_id, section.section, None, None, section.dText, section.startPos, section.endPos, section.desc))
            
        if len(parsed_events) > 0:
            self.dbadapter.save_article_section_events(parsed_events)
            self.dbadapter.update_article_section_parse_status(article_id, section_id, True)
            self.dbadapter.commit()

        return self.wikiHtmlParser.sectionEvents

    def get_missing_sections(self, article_id: int, last_section_id: int):
        return self.dbadapter.missing_article_sections(article_id, last_section_id)

    def get_article_search_matches(self, search : str, max_results : int):
        if search == "":
            return []
        results = self.dbadapter.article_search(search, max_results)
        return results