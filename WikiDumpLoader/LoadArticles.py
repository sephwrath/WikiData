from WikiData.Domain.WikiArticles import extract_file_names, extract_file_articles, extract_article_to_article_tbl
from WikiData.Domain.WikiHtmllParse import WikiHtmlParser
from WikiData.Domain.PgrsTemporal import PgrsTemporal

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('config.ini')

    mydb = connection.MySQLConnection.connect(
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

    extract_article_to_article_tbl(html_file_path, mycursor, json_save_path)