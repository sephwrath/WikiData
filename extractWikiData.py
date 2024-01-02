import tarfile
import mysql.connector
import json
from temporalParse import TemporalParser
import html2text
from bs4 import BeautifulSoup
import re
import datetime as dt



def extract_file_names(file_path, cursor, mydb):
    """
    Extracts file names and member data from a tar.gz file and inserts them into a MySQL database
    so that the individual files in the tar file can be acessed in any order without needing to read the entire file.

    Args:
        file_path (str): The path to the tar.gz file.
        cursor (MySQLCursor): The cursor object for executing MySQL statements.
        mydb (MySQLConnection): The connection object for the MySQL database.

    Returns:
        None
    """
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

def get_last_inserted_dump_details(mycursor):
    select_last_dump = """select Max(dump_file_id) df_id, Max(dump_idx) df_idx
        from article
        where dump_file_id = (select Max(dump_file_id) FROM article);"""
    mycursor.execute(select_last_dump)
    dump_files = mycursor.fetchone()
    # id's start at 1 & dump_index starts at 0
    dump_index = -1
    dump_file_id = 1

    if (dump_files[0] != None):
        dump_index = dump_files[1]
        dump_file_id = dump_files[0]
    
    mycursor.execute("SELECT id, file_name, tar_info FROM dump_file WHERE id >= %s", (dump_file_id,))
    dump_files = mycursor.fetchall()
        
    return (dump_index, dump_files)



def extract_file_articles(file_path, mycursor, mydb):
    insert_article = "INSERT INTO article (title, `update`, dump_file_id, dump_idx, url, redirect, no_dates) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    insert_article_section = "INSERT INTO article_section (article_id, section_id, `tag`, `text`) VALUES (%s, %s, %s, %s)"
    insert_article_section_table_row = "INSERT INTO article_section_table_row (article_id, section_id, row_idx, row_type) values (%s, %s, %s, %s)"
    insert_article_section_table_cell = "INSERT INTO article_section_table_cell (row_id, column_idx, column_span, row_span, text) VALUES (%s,%s,%s, %s, %s)"
    insert_article_section_link = "INSERT INTO article_section_link (article_id, section_id, row_idx, column_idx, start_pos, end_pos, link) values (%s, %s, %s, %s, %s, %s, %s);"
    insert_parsed_event = "INSERT INTO parsed_event (article_id, section_id, row_idx, column_idx, start_date, end_date, date_text, start_pos, end_pos, display_text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
    temporParse = TemporalParser()
    raw_html = ""

    prev_run_df = get_last_inserted_dump_details(mycursor)

    with tarfile.open(file_path, mode="r:gz") as tf:
        for file_rec in prev_run_df[1]:
            # load the member info from the colum as using get members requires reading the whole tarfile
            member = tarfile.TarInfo.frombuf(file_rec[2], tarfile.ENCODING, 'surrogateescape')

            with tf.extractfile(member) as file_input:

                # loop through each of the articles in the files
                for (idx, line) in enumerate(file_input):
                    # skip until we catch up to the last inserted article
                    if (idx <= prev_run_df[0]):
                        continue

                    # for the first line remove the header - the headder starts with the file name
                    if (line.startswith(file_rec[1].encode('ascii'))):
                        line = line[len(file_rec[2]):]
                    article = json.loads(line)

                    raw_html = article["article_body"]["html"]
                    title = article['name']
                    url = article["url"]

                    # replace dashes with hyphens - spacy doesn't recognize dashes
                    raw_html = raw_html.replace("–", "-")
                    
                    # text = ""
                    # for line in raw_html:
                    #     tempLine = line.replace("\\n", "\n").strip() + " "
                    #     # replace double escaped characters
                    #     tempLine = re.sub(r"(\\')", "'", tempLine)
                    #     text += tempLine

                    no_events = True
                    redirect = None
                    now_time = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # easiest way to check for redirects is to see if there is a #REDIRECT in the wikitext
                    wikiText = article["article_body"]["wikitext"]
                    redirects = re.findall(r'#REDIRECT\[\[(.*)\]\]', wikiText)
                    if len(redirects) > 0:
                        print("parsing: {}, redirects {}".format(title, redirects))
                        redirect = redirects[0]
                        insert_values = (title, now_time, file_rec[0], idx, url, redirect, no_events)
                        mycursor.execute(insert_article, insert_values)
                        mydb.commit()
                        continue


                    parsed_html = BeautifulSoup(raw_html, features="html.parser")
                                       
                    temporParse.parse(parsed_html, title)
                    temporParse.parseEvents()
                    print("parsing: {}, sections {}, events {}, links {}".format(title, len(temporParse.saveSections), len(temporParse.sectionEvents), len(temporParse.sectionLinks)))

                    # insert the article sections and events
                    # if the no events then don't insert the sections etc just add the article info
                    if len(temporParse.sectionEvents) > 0 or len(temporParse.sectionLinks) > 0:
                        no_events = False

                    insert_values = (title, now_time, file_rec[0], idx, url, redirect, no_events)
                    mycursor.execute(insert_article, insert_values)
                    article_id = mycursor.lastrowid

                    if not no_events:
                        # insert the article sections
                        insert_sections = []
                        insert_table_rows = []
                        insert_table_columns = []
                        for (sec_idx, section) in enumerate(temporParse.saveSections):
                            # (article_id, section_id `tag`, `text`)
                            #{ 'type': type, 'text': text }
                            section_text = section['text']
                            mycursor.execute(insert_article_section, (article_id, sec_idx, section['type'], section_text))
                            if (section['type'] == temporParse.TYPE_TABLE):
                                # table section = { 'type': self.TYPE_TABLE, 'rows': self.tableRows }                                
                                for (row_idx, row) in enumerate(section['rows']):
                                    #(id, article_id, section_id, row_idx, row_type)
                                    #insert_table_rows.append()
                                    # neet to execute for each row to get the row id.
                                    mycursor.execute(insert_article_section_table_row, (article_id, sec_idx, row_idx, row[0]))
                                    new_row_id = mycursor.lastrowid
                                    for (col_idx, col) in enumerate(row[1]):
                                        # (article_id, section_id, row_idx, column_idx, column_span, row_span, text)
                                        col_span = col[1] if col[1] > 1 else None
                                        row_span = col[2] if col[2] > 1 else None
                                        insert_table_columns.append((new_row_id, col_idx, col_span, row_span, col[0]))
                            
                            
                        if len(insert_table_columns) > 0:
                            mycursor.executemany(insert_article_section_table_cell, insert_table_columns)
                        
                        section_links = []
                        for link in temporParse.sectionLinks:
                            # link = self.sectionLinks.append({ 'section': 'article': 'start': 'end': 'column':  'row': 
                            # (article_id, section_id, row_idx, column_idx, start_pos, end_pos, link)
                            lr = link['row'] if 'row' in link else None
                            lc = link['column'] if 'column' in link else None
                                
                            section_links.append((article_id, link['section'], lr, lc, 
                                                         link['start'], link['end'], link['article']))
                        if len(section_links) > 0:
                            mycursor.executemany(insert_article_section_link, section_links)

                        parsed_events = []
                        for section in temporParse.sectionEvents:
                            # (article_id, section_id, row_idx, column_idx, start_date, end_date, date_text, start_pos, end_pos, display_text
                            # event = { 'section': idx, 'rowIdx': rowIdx, 'columnIdx': columnIdx, 'startPos': startPos, 'endPos': endPos, 'dText': dText, 'desc': desc }
                            er = section['rowIdx'] if 'rowIdx' in section else None
                            ec = section['columnIdx'] if 'columnIdx' in section else None
                            parsed_events.append((article_id, section['section'], er, ec, 
                                                        None, None, section['dText'], section['startPos'], section['endPos'], section['desc']))
                            
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

    #extract_file_names(html_file_path, mycursor, mydb)

    extract_file_articles(html_file_path, mycursor, mydb)

    

