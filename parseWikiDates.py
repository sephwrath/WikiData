import mysql.connector

import re
import datetime as dt


# import requi9red module
import sys
 
# append the path of the
# parent directory
sys.path.append("C:/Users/stephen/Documents/Projects/date-finder/dt_rd_parser")

from timeParser import TimeParser



def extract_dates_from_db(cursor, mydb):

    update_parsed_event = """UPDATE wikidata.parsed_event SET start_date = %s, end_date = %s where id = %s;"""
    mycursor.execute("""select article_id, section_id, row_idx, column_idx, date_text, start_pos, end_pos, id 
        from wikidata.parsed_event where start_date is Null or end_date is Null
        order by article_id, section_id, row_idx, column_idx
        limit 1000;""")
    
    dates_to_parse = mycursor.fetchall()
    time_parser = TimeParser()
    last_article_date = None
    last_article_id = None

    for date_rec in dates_to_parse:
        article_id = date_rec[0]
        section_id = date_rec[1]
        row_idx = date_rec[2]
        column_idx = date_rec[3]
        date_text = date_rec[4]
        start_pos = date_rec[5]
        end_pos = date_rec[6]
        pe_id = date_rec[7]
        
        print("article_id: ", article_id, " section_id: ", section_id, " row_idx: ", row_idx, " column_idx: ", column_idx, " date_text: ", date_text, " start_pos: ", start_pos, " end_pos: ", end_pos)

        try:
            time_parser.set_refrence_date(last_article_date.to_datetime('start') if last_article_id == article_id else dt.datetime.now()) 
            date = time_parser.parse(date_text)

            # only commit after we have parsed all the dates in the article
            if last_article_id != article_id and last_article_id is not None:
                mydb.commit()

            mycursor.execute(update_parsed_event, (date.to_timestamp('start'), date.to_timestamp('end'), pe_id))
            

            last_article_date = date
            last_article_id = article_id

            

            print("result: " + str(date))



        except Exception as e:
            print(e.msg, " ", get_date_context(mycursor, article_id, section_id, row_idx, column_idx))


def get_date_context(cursor, article_id, section_id, row_idx, column_idx):
    if row_idx and column_idx:
        cursor.execute("""SELECT tc.text FROM wikidata.article_section_table_cell tc, wikidata.article_section_table_row tr
            where tr.article_id = %s and tr.section_id = %s and tr.row_idx = %s and tr.id = tc.row_id and tc.column_idx = %s;""",
            (article_id, section_id, row_idx, column_idx))
        parseText = cursor.fetchall()
        
    else:
        cursor.execute("""SELECT text FROM wikidata.article_section where article_id = %s and section_id = %s;""", 
                     (article_id, section_id))
        parseText = cursor.fetchall()
    return parseText[0][0]

if __name__ == "__main__":
    mydb = mysql.connector.connect(
            host="localhost",
            user="temporal",
            password="***********",
            database="wikidata"
        )

    mycursor = mydb.cursor()

    extract_dates_from_db(mycursor, mydb)
