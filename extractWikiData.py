import tarfile
import mysql.connector
import json
from temporalParse import TemporalParser
import html2text
from bs4 import BeautifulSoup
import re
import datetime as dt

def extract_file_names(file_path, cursor, mydb):
    sql_ins_dump_file = "INSERT INTO dump_file (file_name) VALUES (%s)"
    
    with tarfile.open(file_path, mode="r:gz") as tf:
        while True:
            member = tf.next()
            if member is None:
                break
            if member.isfile():
                #print(member.name)
                val = (member.name,)
                cursor.execute(sql_ins_dump_file, (val))
                mydb.commit()


def extract_file_articles(file_path, mycursor, mydb):
    #mycursor.exequte("SELECT dump_file_id, dump_idx FROM dump_file")")

    mycursor.execute("SELECT id, file_name FROM dump_file")
    dump_files = mycursor.fetchall()

    insert_article = "INSERT INTO article (title, update, dump_file_id, dump_idx) VALUES (%s, %s, %s, %s)"
    
    temporParse = TemporalParser()
    raw_html = ""

    with tarfile.open(file_path, mode="r:gz") as tf:
        for file_rec in dump_files:
            member = tf.getmember(file_rec[1])
            #print(x)

            with tf.extractfile(member) as file_input:

                # loop through each of the articles in the files
                for (idx, line) in enumerate(file_input):
                    article = json.loads(line)

                    raw_html = article["article_body"]["html"]
                    
                    text = ""
                    for line in raw_html:
                        tempLine = line.replace("\\n", "\n").strip() + " "
                        # replace double escaped characters
                        tempLine = re.sub(r"(\\')", "'", tempLine)
                        text += tempLine

                    parsed_html = BeautifulSoup(text, features="html.parser")
                    #htmltext = parsed_html.encode('utf-8').decode('utf-8','ignore')
                    title = parsed_html.title.text
                    mycursor.execute(insert_article, (title, dt.dateTime.now(), file_rec[0], idx ))
                    
                    temporParse.parse(parsed_html)

                    temporParse.parseEvents()

                    # insert the article sections and events


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

    

