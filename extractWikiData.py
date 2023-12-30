import tarfile
import mysql.connector
import json
from temporalParse import TemporalParser
import html2text
from bs4 import BeautifulSoup
import re
import datetime as dt



def extract_file_names(file_path, cursor, mydb):
    sql_ins_dump_file = '''INSERT INTO dump_file (file_name, tar_info) VALUES (%s, _binary %s)'''
    
    with tarfile.open(file_path, mode="r:gz") as tf:
        while True:
            member = tf.next()
            if member is None:
                break
            if member.isfile():
                #print(member.name)
                val = (member.name, member.tobuf())
                cursor.execute(sql_ins_dump_file, val)
                mydb.commit()


def extract_file_articles(file_path, mycursor, mydb):
    #mycursor.exequte("SELECT dump_file_id, dump_idx FROM dump_file")")

    mycursor.execute("SELECT id, file_name, tar_info FROM dump_file")
    dump_files = mycursor.fetchall()

    insert_article = "INSERT INTO article (title, `update`, dump_file_id, dump_idx, url, redirect, no_dates) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    insert_article_section = "INSERT INTO article_section (article_id, section_id `tag`, `text`) VALUES (%s, %s, %s, %s)"
    insert_article_section_table = "INSERT INTO article_section_table (article_id, section_id, row_idx, column_idx, text) VALUES (%s, %s, %s, %s, %s)"
    insert_article_section_link = "INSERT INTO article_section_link (article_id, section_id, row_idx, column_idx, start_pos, end_pos, link) values (%s, %s, %s, %s, %s, %s, %s);"
    insert_parsed_event = "INSERT INTO parsed_event (article_id, section_id, row_idx, column_idx, start_date, end_date, date_text, start_pos, end_pos, display_text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
    temporParse = TemporalParser()
    raw_html = ""

    with tarfile.open(file_path, mode="r:gz") as tf:
        for file_rec in dump_files:
            # load the member info from the colum as using get members requires reading the whole tarfile
            member = tarfile.TarInfo.frombuf(file_rec[2], tarfile.ENCODING, 'surrogateescape')

            with tf.extractfile(member) as file_input:

                # loop through each of the articles in the files
                for (idx, line) in enumerate(file_input):
                    # for the first line remove the header
                    if (line.startswith(file_rec[1].encode('ascii'))):
                        line = line[len(file_rec[2]):]
                    article = json.loads(line)

                    raw_html = article["article_body"]["html"]
                    title = article['name']
                    url = article["url"]

                    
                    # text = ""
                    # for line in raw_html:
                    #     tempLine = line.replace("\\n", "\n").strip() + " "
                    #     # replace double escaped characters
                    #     tempLine = re.sub(r"(\\')", "'", tempLine)
                    #     text += tempLine

                    parsed_html = BeautifulSoup(raw_html, features="html.parser")
                    now_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    temporParse.parse(parsed_html)

                    temporParse.parseEvents()

                    # insert the article sections and events
                    # if the no events
                    no_events = True
                    redirect = None

                    if len(temporParse.sectionEvents) > 0:
                        no_events = False

                    insert_values = (title, now_time, file_rec[0], idx, url, redirect, no_events)
                    mycursor.execute(insert_article, insert_values)
                    article_id = mycursor.lastrowid

                    if not no_events:
                        # insert the article sections
                        insert_sections = []
                        insert_section_tables = []
                        for (sec_idx, section) in enumerate(temporParse.saveSections):
                            # (article_id, section_id `tag`, `text`)
                            #{ 'type': type, 'text': text }
                            section_text = None
                            if (section['type'] == temporParse.TYPE_TABLE):
                                # table section = { 'type': self.TYPE_TABLE, 'headder': self.tableHeader, 'rows': self.tableRows }
                                for (hddr_idx, hddr_col) in enumerate(temporParse.section['headder']):
                                    # (article_id, section_id, row_idx, column_idx, text)
                                    insert_section_tables.append((article_id, sec_idx, 0, hddr_idx, hddr_col))
                                for (row_idx, row) in enumerate(temporParse.section['rows']):
                                    for (col_idx, col) in enumerate(row):
                                        # (article_id, section_id, row_idx, column_idx, text)
                                        insert_section_tables.append((article_id, sec_idx, row_idx, col_idx, col))
                            else:
                                section_text = section['text']
                            
                            insert_sections.append((article_id, sec_idx, section['type'], section_text))

                        if len(insert_sections) > 0:
                            mycursor.executemany(insert_article_section, insert_sections)
                        if len(insert_section_tables) > 0:
                            mycursor.executemany(insert_article_section_table, insert_section_tables)
                        
                        section_links = []
                        for link in temporParse.sectionLinks:
                            # link = self.sectionLinks.append({ 'section': 'article': 'start': 'end': 'column':  'row': 
                            # (article_id, section_id, row_idx, column_idx, start_pos, end_pos, link)
                            section_links.append((article_id, link['section'], link['row'], link['column'], 
                                                         link['start'], section['end'], section['article']))
                        if len(section_links) > 0:
                            mycursor.executemany(insert_article_section_link, section_links)

                        parsed_events = []
                        for section in temporParse.sectionEvents:
                            # (article_id, section_id, row_idx, column_idx, start_date, end_date, date_text, start_pos, end_pos, display_text
                            # event = { 'section': idx, 'rowIdx': rowIdx, 'columnIdx': columnIdx, 'startPos': startPos, 'endPos': endPos, 'dText': dText, 'desc': desc }
                            parsed_events.append(article_id, section['section'], section['rowIdx'], section['columnIdx'], 
                                                        None, None, section['dText'], section['startPos'], section['endPos'], section['desc'])
                            
                        if len(parsed_events) > 0:
                            mycursor.executemany(insert_parsed_event, parsed_events)
                            
                    

                    mydb.commit()


if __name__ == "__main__":
    mydb = mysql.connector.connect(
            host="localhost",
            user="temporal",
            password="nutsackcoffeedunk1957",
            database="wikidata"
        )
    
    html_file_path = "C:\\Users\\stephen\\Documents\\enwiki-NS0-20231020-ENTERPRISE-HTML.json.tar.gz"

    mycursor = mydb.cursor()

    extract_file_names(html_file_path, mycursor, mydb)

    #extract_file_articles(html_file_path, mycursor, mydb)

    

