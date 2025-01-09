import tarfile
import mysql.connector
import json
from .wikiHtmllParse import WikiHtmlParser
import html2text
from bs4 import BeautifulSoup
import re
import os
import sys
import datetime as dt
import time
import configparser

sys.path.append("./date-finder/dt_rd_parser")

from dt_rd_parser.timeParser import TimeParser



ALGORYTHM_UPDATE_DATE = dt.datetime.strptime('2024-12-27', '%Y-%m-%d')

time_parser = TimeParser()



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
    sql_ins_dump_file = '''INSERT INTO dump_file (file_name, tar_info, offset, offset_data) VALUES (%s, _binary %s, %s, %s)'''
    sql_check_exists = '''SELECT id FROM dump_file WHERE file_name = %s'''
    sql_update_dump_file = '''UPDATE dump_file SET tar_info = _binary %s, offset = %s, offset_data = %s WHERE id = %s'''
    
    with tarfile.open(file_path, mode="r:gz") as tf:
        while True:
            member = tf.next()
            if member is None:
                break
            if member.isfile():
                cursor.execute(sql_check_exists, (member.name,))
                file_id_rec = cursor.fetchone()

                if (file_id_rec[0] != None):
                    val = (member.tobuf(), member.offset, member.offset_data, file_id_rec[0])
                    cursor.execute(sql_update_dump_file, val)
                else:
                #print(member.name)
                    val = (member.name, member.tobuf(), member.offset, member.offset_data)
                    cursor.execute(sql_ins_dump_file, val)
                mydb.commit()

def get_missing_dump_details(mycursor):

    """
    Retrieves details of the last inserted articles dump file and gets all the dump files after that

    Args:
        mycursor (MySQLCursor): The cursor object used to execute MySQL statements.

    Returns:
        tuple: A tuple containing the last dump index (int) and a list of dump files 
               (list of tuples), where each tuple represents a dump file with 
               fields (id, file_name, tar_info, offset, offset_data).
    """


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
    
    mycursor.execute("SELECT id, file_name, tar_info, offset, offset_data FROM dump_file WHERE id >= %s order by id", (dump_file_id,))
    dump_files = mycursor.fetchall()
        
    return (dump_index, dump_files)

def get_article_count(file_path,mycursor):

    prev_run_df = get_missing_dump_details(mycursor)

    with tarfile.open(file_path, mode="r:gz") as tf:
        # load the member info from the colum as using get members requires reading the whole tarfile
        
        for file_rec in prev_run_df[1]:
            member = tarfile.TarInfo.frombuf(file_rec[2], tarfile.ENCODING, 'surrogateescape')
            member.offset = file_rec[3]
            member.offset_data = file_rec[4]

            with tf.extractfile(member) as file_input:
                num_lines = sum(1 for _ in file_input)

            print("{} articles in {}".format(num_lines, file_rec[1]))

def extract_tar_files(tar_path, save_path):
    files = os.listdir(save_path)
    with tarfile.open(tar_path, mode="r:gz") as tf:
        while True:
            member = tf.next()
            if member is None:
                break
            if member.isfile():
                with tf.extractfile(member) as file_input:
                    if member.name not in files:
                        with open(save_path + member.name, "wb") as file_output:
                            file_output.write(file_input.read())

def create_tar_files(save_path):
    files = os.listdir(save_path)

    for file in files:
        if file.endswith(".tar.gz"):
            continue
        with tarfile.open(save_path + file + ".tar.gz", mode="w:gz") as tar:
            tar.add(save_path + file, arcname=file)
            tar.close()

        os.remove(save_path + file)

def write_article_lines_to_db(line, dump_id, dump_idx, mycursor):
    insert_article = "INSERT INTO article (title, `update`, dump_file_id, dump_idx, url, redirect, no_dates, wiki_update_ts, description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # timing variables
    t_count = 0
    t_total = 0
    t_s = 0
    t_e = 0
    t_s = time.time()

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

        insert_values = (title, now_time, dump_id, dump_idx, url, redirect, no_events, mod_date, description)
        mycursor.execute(insert_article, insert_values)
        if 'redirects' in article:
            for redirect in article['redirects']:
                if 'url' not in redirect:
                    redirect['url'] = None
                if 'name' in redirect:
                    mycursor.execute(insert_article, (redirect['name'], now_time, None, None, redirect['url'], title, no_events, mod_date, None))


        mydb.commit()
    except mysql.connector.Error as err:
        print("Mysql error: {}".format(err))

    t_e = time.time()
    t_tot = t_e - t_s
    t_count += 1
    t_total += t_tot
    t_avg = t_total / t_count
    print("title: {}, processing time: {}. Total time: {}, average time for {} articles: {}".format(title, t_tot, t_total, t_count, t_avg))


# get just the article details for the article_table 
def extract_atticle_to_article_tbl(file_path, mycursor, mydb, json_save_path):
    prev_run_df = get_missing_dump_details(mycursor)
    # 0 - dump_idx - the last article inserted into the database
    # 1 - dump_files - id, file_name, tar_info, offset, offset_data
    
    file_start_offset = prev_run_df[0]
    first_file_id = prev_run_df[1][0][0]

    # if we are using the json files then just read the file
    if json_save_path is not None:
        # save the json to a file
        for file_rec in prev_run_df[1]:

            if file_rec['id'] > first_file_id:
                    file_start_offset = -1

            with open(json_save_path + file_rec['file_name'], "r", encoding=tarfile.ENCODING) as file_output:
                for (idx, line) in enumerate(file_output):
                    if (idx <= file_start_offset):
                        continue
                    write_article_lines_to_db(line, file_rec['id'], idx, mycursor)
    else:

        with tarfile.open(file_path, mode="r:gz") as tf:
            for file_rec in prev_run_df[1]:
                # load the member info from the colum as using get members requires reading the whole tarfile
                member = tarfile.TarInfo.frombuf(file_rec['tar_info'], tarfile.ENCODING, 'surrogateescape')
                # the offset and offset_data are not saved by TarInfo.tobuf() or restored by TarInfo.frombuf() so they need to be set manually
                # this seems to be a bug in the Tare file library - TODO submit a bug report
                member.offset = file_rec['offset']
                member.offset_data = file_rec['offset_data']

                # reset the line offset for subsequent files
                if file_rec['id'] > first_file_id:
                    file_start_offset = -1

                with tf.extractfile(member) as file_input:

                    # loop through each of the articles in the files
                    for (idx, line) in enumerate(file_input):
                        # skip until we catch up to the last inserted article
                        if (idx <= file_start_offset):
                            continue
                        write_article_lines_to_db(line, file_rec['id'], idx, mycursor)
                    

def parse_article(article_id, line, mycursor, mydb, wikiHtmlParser):
    #insert_article = "INSERT INTO article (title, `update`, dump_file_id, dump_idx, url, redirect, no_dates) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    update_article = "UPDATE article SET `update` = %s, redirect = %s, no_dates = %s, err = %s WHERE id = %s"

    insert_article_section = """INSERT INTO article_section (article_id, section_id, tag, ext_text_count, parent_section_id, 
        row_idx, column_idx, row_span, column_span, format, text) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    insert_article_section_ext_text = "INSERT INTO article_section_ext_text (article_id, section_id, count_id, text) values (%s, %s, %s, %s)"
    insert_article_section_link = "INSERT INTO article_section_link (article_id, section_id, start_pos, end_pos, link) values (%s, %s, %s, %s, %s);"

    # timing variables
    t_s = 0
    t_e = 0
    t_s = time.time()

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
                        
    wikiHtmlParser.parse(parsed_html, title)
    #wikiHtmlParser.parseEvents()
    print("parsing: {}, sections {}, events {}, links {}".format(title, len(wikiHtmlParser.saveSections), len(wikiHtmlParser.sectionEvents), len(wikiHtmlParser.sectionLinks)))

    # insert the article sections and events
    # if the no events then don't insert the sections etc just add the article info
    #if len(wikiHtmlParser.sectionEvents) > 0 or len(wikiHtmlParser.sectionLinks) > 0:
    #    no_events = False

    if not no_events:
        # insert the article sections
        insert_table_columns = []
        for (sec_idx, section) in enumerate(wikiHtmlParser.saveSections):
            # (article_id, section_id `tag`, `text`)
            #{ 'type': type, 'text': text }
            # split the section text into chunks for saving in the database
            section_text = section['text']
            section_chunks = []
            if section_text == "":
                section_chunks.append("")
            else:
                while section_text:
                    chunk, section_text = section_text[:14000], section_text[14000:]
                    section_chunks.append(chunk)
            
            ext_type_count = len(section_chunks) - 1

            #returned table values - column_span, row_span, row, column, type
            if section['type'] == wikiHtmlParser.TYPE_TABLE_CELL or section['type'] == wikiHtmlParser.TYPE_LIST_ITEM:
                # (article_id, section_id, tag, ext_text_count, parent_section_id, row_idx, column_idx, row_span, column_span, format, text)
                section_tuple = (article_id, sec_idx, section['type'], ext_type_count, section['parent_section'],
                    section['row'], section['column'], section['row_span'], section['column_span'], section['format'], section_chunks[0])
            else:
                # (article_id, section_id, tag, ext_text_count, parent_section_id, row_idx, column_idx, row_span, column_span, format, text)
                section_tuple = (article_id, sec_idx, section['type'], ext_type_count, section['parent_section'],
                    None, None, None, None, None, section_chunks[0])
            
            mycursor.execute(insert_article_section, section_tuple)

            for (chunk_idx, section_chunk) in enumerate(section_chunks[1:]):
                mycursor.execute(insert_article_section_ext_text, (article_id, sec_idx, chunk_idx, section_chunk))

        section_links = []
        for link in wikiHtmlParser.sectionLinks:
            # (article_id, section_id, start_pos, end_pos, link)                
            section_links.append((article_id, link['section'], link['start'], link['end'], link['article']))
        if len(section_links) > 0:
            mycursor.executemany(insert_article_section_link, section_links)
            
    insert_values = (now_time, redirect, no_events, 'UP_TO_DATE', article_id)
    mycursor.execute(update_article, insert_values)
    #article_id = mycursor.lastrowid
    mydb.commit()

    t_e = time.time()
    t_tot = t_e - t_s
    print("title: {}, processing time: {}.".format(title, t_tot))


def extract_article_detail_by_id(article_id, file_path, cursor, mydb, json_save_path = None, wikiHtmlParser = WikiHtmlParser()):

    select_article = """select a.id, title, `update`, dump_idx, url, redirect, no_dates, wiki_update_ts, err, 
        df.file_name, df.tar_info, df.offset, df.offset_data
        from article a, dump_file df
        where dump_file_id = df.id 
        and a.id = %s"""
    delete_article_section = "delete from article_section where article_id = %s"
    delete_article_section_ext_text = "delete from article_section_ext_text where article_id = %s"
    delete_article_section_link = "delete from article_section_link where article_id = %s"
    delete_parsed_event = "delete from parsed_event where article_id = %s"

    select_article_section = "select * from article_section where article_id = %s"
    select_article_section_ext_text = "select * from article_section_ext_text where article_id = %s"
    select_article_section_link = "select * from article_section_link where article_id = %s"
    select_parsed_event = "select * from parsed_event where article_id = %s"
    # check if the article needs to be updated
    cursor.execute(select_article, (article_id,))
    article_rec = cursor.fetchone()

    if article_rec is None:
        print("article {} not found".format(article_id))
        return

    if article_rec['err'] != 'UP_TO_DATE':
        # if it does then delete all the sections, rows, cells and links
        cursor.execute(delete_parsed_event, (article_id,))
        cursor.execute(delete_article_section_link, (article_id,))
        cursor.execute(delete_article_section_ext_text, (article_id,))
        cursor.execute(delete_article_section, (article_id,))
        mydb.commit()

        # if we are using the json files then just read the file
        if json_save_path is not None:
            # save the json to a file
            with open(json_save_path + article_rec['file_name'], "r", encoding=tarfile.ENCODING) as file_output:
                for (idx, line) in enumerate(file_output):
                    if (idx < article_rec['dump_idx']):
                        continue
                    parse_article(article_id, line, cursor, mydb, wikiHtmlParser)
                    break
        else:
            with tarfile.open(file_path, mode="r:gz") as tf:
                # load the member info from the colum as using get members requires reading the whole tarfile
                member = tarfile.TarInfo.frombuf(article_rec['tar_info'], tarfile.ENCODING, 'surrogateescape')
                # the offset and offset_data are not saved by TarInfo.tobuf() or restored by TarInfo.frombuf() so they need to be set manually
                # this seems to be a bug in the Tare file library - TODO submit a bug report
                member.offset = article_rec['offset']
                member.offset_data = article_rec['offset_data']

                with tf.extractfile(member) as file_input:

                    # loop through each of the articles in the files
                    for (idx, line) in enumerate(file_input):
                        # skip until we catch up to the last inserted article
                        if (idx < article_rec['dump_idx']):
                            continue

                        parse_article(article_id, line, mycursor, mydb, wikiHtmlParser)
                        break
    
    cursor.execute(select_parsed_event, (article_id,))
    article_events = cursor.fetchall()
    cursor.execute(select_article_section_link, (article_id,))
    article_links = cursor.fetchall()
    cursor.execute(select_article_section_ext_text, (article_id,))
    article_ext_text = cursor.fetchall()
    cursor.execute(select_article_section, (article_id,))
    article_sections = cursor.fetchall()

    return (article_rec, article_sections, article_ext_text, article_links, article_events)

def extract_remaining_article_sections_by_id(article_id, cursor):
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
            section['text'] = section['text'] + ext_text['text']
    return remaining_sections

def parse_section_events(article_id, section_id, section_text, mydb, mycursor, wikiHtmlParser):
    insert_parsed_event = "INSERT INTO parsed_event (article_id, section_id, start_date, end_date, date_text, start_pos, end_pos, display_text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    update_section = "UPDATE article_section SET is_parsed = 'Y' WHERE article_id = %s and section_id = %s"
    wikiHtmlParser.sectionEvents = []
    wikiHtmlParser.extract_events_spacy(section_text, section_id)

    parsed_events = []
    for section in wikiHtmlParser.sectionEvents:
        # (article_id, section_id, start_date, end_date, date_text, start_pos, end_pos, display_text
        # event = { 'section': idx,  'startPos': startPos, 'endPos': endPos, 'dText': dText, 'desc': desc }
        parsed_events.append((article_id, section['section'], None, None, section['dText'], section['startPos'], section['endPos'], section['desc']))
        
    if len(parsed_events) > 0:
        mycursor.executemany(insert_parsed_event, parsed_events)
        mycursor.execute(update_section, (article_id, section_id))
        mydb.commit()

    return wikiHtmlParser.sectionEvents

def get_article_search_matches(search, max_results, cursor):
    if search == "":
        return []
    if '%' not in search:
        search = search + '%'

    #search_articles = """SELECT a.id, title, url, redirect, no_dates, wiki_update_ts, err
    #    FROM article a
    #    WHERE title like %s
    #    LIMIT %s"""
    search_articles= """select art.title as sub_title,
            case when rdr.id is not null then rdr.id else art.id end as id,
            case when rdr.title is not null then rdr.title else art.title end as title,
            case when rdr.`description` is not null then rdr.`description` else art.`description` end as `description`
        from article as art
        left outer join article as rdr  on art.redirect = rdr.title
        where art.title like %s limit %s"""
    cursor.execute(search_articles, (search, max_results))
    return cursor.fetchall()


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('config.ini')

    mydb = mysql.connector.connect(
            host=config.get('General', 'host'),
            user=config.get('General', 'user'),
            password=config.get('General', 'password'),
            database=config.get('General', 'database')
        )
    
    html_file_path = config.get('General', 'html_file_path')
    json_save_path = config.get('General', 'json_save_path')

    mycursor = mydb.cursor(dictionary=True)
    wikiHtmlParser = WikiHtmlParser()

    #extract_file_names(html_file_path, mycursor, mydb)

    #extract_file_articles(html_file_path, mycursor, mydb)

    extract_atticle_to_article_tbl(html_file_path, mycursor, mydb, json_save_path)

    #extract_tar_files(html_file_path, json_save_path)

    #get_article_count(html_file_path, json_save_path)

    #extract_article_detail_by_id(6386494, html_file_path, mycursor, mydb, json_save_path, wikiHtmlParser)

    #create_tar_files(json_save_path)

    #get_article_count(html_file_path, mycursor)

    """with tarfile.open(html_file_path, mode="r:gz") as tf:
        while True:
            member = tf.next()

            file_input = tf.extractfile(member)
            line = file_input.readline()[:1000]
            print(line)

            buffer = member.tobuf(format=tf.format, encoding=tf.encoding, errors=tf.errors)
            member2 = tarfile.TarInfo.frombuf(buffer, tf.encoding, tf.errors)

            member2.offset = member.offset
            member2.offset_data = member.offset_data

            file_input = tf.extractfile(member2)
            line = file_input.readline()[:1000]
            print(line)

    mycursor.execute("SELECT id, file_name, tar_info FROM dump_file WHERE id = 2 order by id")
    dump_files = mycursor.fetchall()

    member = tarfile.TarInfo.frombuf(dump_files[0][2], tarfile.ENCODING, 'surrogateescape')

    print(member)"""

    

    

