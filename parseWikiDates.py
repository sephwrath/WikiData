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
    mycursor.execute("""select article_id, section_id, row_idx, column_idx, date_text, start_pos, end_pos 
        from wikidata.parsed_event where start_date is Null or end_date is Null
        order by id
        limit 1000;""")
    
    dates_to_parse = mycursor.fetchall()
    time_parser = TimeParser()

    for date_rec in dates_to_parse:
        article_id = date_rec[0]
        section_id = date_rec[1]
        row_idx = date_rec[2]
        column_idx = date_rec[3]
        date_text = date_rec[4]
        start_pos = date_rec[5]
        end_pos = date_rec[6]
        
        print("article_id: ", article_id, " section_id: ", section_id, " row_idx: ", row_idx, " column_idx: ", column_idx, " date_text: ", date_text, " start_pos: ", start_pos, " end_pos: ", end_pos)

        date = time_parser.parse(date_text)

        print("result: " + str(date))



if __name__ == "__main__":
    mydb = mysql.connector.connect(
            host="localhost",
            user="temporal",
            password="nutsackcoffeedunk1957",
            database="wikidata"
        )

    mycursor = mydb.cursor()

    extract_dates_from_db(mycursor, mydb)