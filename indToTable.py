import mysql.connector

def do_main():
    mydb = mysql.connector.connect(
        host="localhost",
        user="temporal",
        password="nutsackcoffeedunk1957",
        database="wiki_index"
    )
    
    toc_file_name = None
    field_num = 3
    separator = ':'
    verbose = False
    insCursor = mydb.cursor()

    #delSql = "DELETE FROM wikipageindex"
    #insCursor.execute(delSql)
    #mydb.commit()

    getLastInd = "select * from chunkoffsets where id = (SELECT max(id) from chunkoffsets);"

    insCursor.execute(getLastInd)

    lastPosition = insCursor.fetchone()

   
    insPg = "INSERT INTO wikipageindex (title, chunkOffset, articleID) VALUES (%s, %s, %s)"
    insChunk = "INSERT INTO chunkoffsets (id, start, end) VALUES (%s, %s, %s)"

    dir = "C:\\Users\\steph\\Documents\\Projects\\enwiki-20230101-pages-articles-multistream\\"
    file = "enwiki-20230101-pages-articles-multistream-index"

    in_file = open(dir+file+".txt", 'r', encoding="utf-8")
    index = 0
    currentOffset = None
    offsetIndex = 0
    if lastPosition:
        offsetIndex = lastPosition[0] + 1
    commitArrAtricles = []

    for line in in_file:
        stripped = line.rstrip('\n')
        fields = stripped.split(separator, field_num -1)
        if not lastPosition or int(fields[0]) > lastPosition[1]:

            if currentOffset != fields[0] :
                if currentOffset != None:
                    print("commit to db index: ", index)
                    insCursor.execute(insChunk, (offsetIndex, currentOffset, fields[0]))
                    insCursor.executemany(insPg, commitArrAtricles)
                    mydb.commit()
                    commitArrAtricles = []
                    offsetIndex = offsetIndex + 1
                currentOffset = fields[0]
            # append after the commit so the new item is related to the new offset
            commitArrAtricles.append((fields[2], offsetIndex, fields[1]))

        index = index + 1

    in_file.close()
    exit(0)

if __name__ == "__main__":
    do_main()