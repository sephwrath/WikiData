from mwparserfromhtml import HTMLDump
import html2text
from dateparser import parse
import os
from bs4 import BeautifulSoup
import spacy, re
#import en_core_web_trf




class TemporalParser:
    def __init__(self):
        self.TYPE_MAIN_TITLE = "MAIN_TITLE"
        self.TYPE_MAIN_IMAGE_URL = "MAIN_IMAGE_URL"
        self.TYPE_TITLE = "TITLE"
        self.TYPE_PARAGRAPH = "PARAGRAPH"
        self.TYPE_IMAGE = "IMAGE"
        self.TYPE_TABLE = "TABLE"
        self.TYPE_QUOTE = "QUOTE"
        self.TYPE_SUBTITLE = "SUBTITLE"
        self.TYPE_SUB_SUBTITLE = "SUB_SUBTITLE"
        self.TYPE_LIST = "LIST"
        self.TYPE_LIST_ITEM = "LIST_ITEM"
        self.LIST_TYPE_BULLETED = "BULLETED"
        self.LIST_TYPE_NUMBERED = "NUMBERED"
        self.LIST_TYPE_INDENTED = "INDENTED"
        
        self.p = re.compile(r'\[\d+\]')

        self.nlp = spacy.load("en_core_web_trf")

    def reset_parser(self):
        self.saveSections = []
        self.sectionLinks = []
        self.sectionEvents = []
        # track to number of characters for the current section so we know where the link should be inserted
        self.linkOffset = 0
        self.soup = None
        self.currentSection = None
        self.tableRows = []
        self.currentRow = []

        # set a context for some of the items - depending on what a parent is we might generate different strings
        self.parent_context = None

    def parse(self, soup, title):
        self.reset_parser()
        self.soup = soup
        if (self.soup.find(id="References")):
            self.soup.find(id="References").findParent(name="section").clear()
        if (self.soup.find(id="External_links")):
            self.soup.find(id="External_links").findParent(name="section").clear()
        self.generateSection(self.TYPE_TITLE, title)

        for bodychild in self.soup.find('body').children:
            self.parseNodes(bodychild)

    def generateSection(self, type, text):
        self.linkOffset = 0
        self.currentSection == None
        nodeSection =  { 'type': type, 'text': text }
    
        if (nodeSection and text != ""):
            self.saveSections.append(nodeSection)
        #print(nodeSection)

    def generateLinkText(self, linkNode):
        strippedText = linkNode.text.strip()

        if not linkNode.attrs['href'].startswith("./File:"):
        
            if (self.parent_context == self.TYPE_TABLE):
                self.sectionLinks.append({ 'section': len(self.saveSections)-1,
                                   'article': linkNode.attrs['href'], 'start': (self.linkOffset + 1),
                                    'end': (self.linkOffset + len(strippedText) + 1),
                                    'column': len(self.currentRow), 'row': len(self.tableRows) } )
            else:
                self.sectionLinks.append({ 'section': len(self.saveSections)-1,
                                   'article': linkNode.attrs['href'], 'start': (self.linkOffset + 1),
                                    'end': (self.linkOffset + len(strippedText) + 1) } )

        return strippedText
    
    def parseChildren(self, node, leading=None, trailing=None):
        nodeText = ""
        for sectionChild in node.children:
            nodeText += self.parseNodes(sectionChild)
            self.linkOffset = len(nodeText)

        if nodeText.strip() == "":
            return ""
        if leading :
            nodeText = leading + nodeText
        if trailing:
            nodeText = nodeText + trailing
        self.linkOffset = len(nodeText)
        return nodeText

    def parseNodes(self,node):
        nodeText = ""
        # print(node.name)
        if (node.name == None):
            if node.text.startswith("<span"):
                return ""
            return node.text.strip()
        if (node.name == "p"):
            self.parent_context = self.TYPE_PARAGRAPH
            self.currentSection = self.TYPE_PARAGRAPH
            for bodychild in node.children:
                
                newText = self.parseNodes(bodychild)
                if newText != "":
                    nodeText += newText + " "
                    self.linkOffset = len(nodeText)
            self.generateSection(self.TYPE_PARAGRAPH,nodeText)

        elif (node.name == "a"):
            return self.generateLinkText(node)
        
        elif (node.name == "b" or node.name == "strong"):
            return self.parseChildren(node, "**", "**")
        
        elif (node.name == "i" or node.name == "em"):
            return self.parseChildren(node, "*", "*")
            
        elif (node.name == "h2"):
            self.generateSection(self.TYPE_TITLE, node.text.strip())

        elif (node.name == "h3"):
            self.generateSection(self.TYPE_SUBTITLE, node.text.strip())

        elif (node.name == "h4"):
            self.generateSection(self.TYPE_SUB_SUBTITLE, node.text.strip())

        elif (node.name == "section"):
            nodeText = self.parseChildren(node)
            #if nodeText != "":
            #    self.generateSection(self.TYPE_PARAGRAPH, nodeText)
        elif (node.name == "span" or node.name == "abbr" or node.name == "u" or node.name == "div"):
            return self.parseChildren(node)

        elif (node.name == "ul" or node.name == "ol" or node.name == "dl"):
            nodeText = self.parseChildren(node)
            if (self.parent_context == self.TYPE_PARAGRAPH):
                #if nodeText != "":
                self.generateSection(self.TYPE_LIST, nodeText)
            else:
                return nodeText

        elif (node.name == "li" or node.name == "dt" or node.name == "dd"):
            return self.parseChildren(node, " - ", "\n")
        
        elif (node.name == "blockquote"):
            return self.parseChildren(node, " > ")
        
        elif (node.name == "table"):
            if 'class' in node.attrs and "infobox" in node.attrs['class']:
                return ""
            else: 
                
                self.parseTable(node)
        elif (node.name == "img"):
            return ""
        else :
            return ""
        
        return ""
    
    def parseTable(self, table):
        """
        For each section below the <table> tag go through and parse
        after resetting the table collections
        
        Args:
            table (TableElement): The table element to be parsed.
        
        Returns: None
        """
        self.tableRows = []
        self.currentRow = []
        self.currentColIdx = 0
        # used as an indicator when processing links
        self.parent_context = self.TYPE_TABLE
        self.row_type = None
        self.caption = None
        for sectionChild in table.children:
            self.parseTablePart(sectionChild)
    
        nodeSection =  { 'type': self.TYPE_TABLE, 'rows': self.tableRows, 'text': self.caption }
        if (len(self.tableRows) > 0):
            self.saveSections.append(nodeSection)
        self.parent_context = None
        print(nodeSection)

    def appendTableColumn(self, node, cell_text):
        # handle column spans
        column_span = int(node.attrs['colspan']) if 'colspan' in node.attrs and node.attrs['colspan'].isdigit() else 1
        row_span = int(node.attrs['rowspan']) if 'rowspan' in node.attrs and node.attrs['rowspan'].isdigit() else 1

        self.currentRow.append((cell_text, column_span, row_span))
        return column_span
    
    def parseTablePart(self, node):
        """
        Parses, thead, tbody, tr by iteratively calling iself then  th, td by using the parse children method
        rows that are are spaned are divided to give each row it's own value

        Parameters:
            node (Node): The node representing the table part to be parsed.

        Returns:
            None
        """
        if (node.name == "thead" or node.name == "tbody"):
            for sectionChild in node.children:
                self.parseTablePart(sectionChild)
        elif (node.name == "tr"):
            for sectionChild in node.children:
                self.parseTablePart(sectionChild)
            
            # once the row is complete add it to the list
            self.tableRows.append((self.row_type, self.currentRow))
            self.currentRow = []

        elif (node.name == "th"):
            self.row_type = 'th'
            self.appendTableColumn(node, self.parseChildren(node))
            
        elif (node.name == "caption"):
            self.caption = self.parseChildren(node)
        elif (node.name == "td"):
            self.row_type = 'td'
            self.appendTableColumn(node, self.parseChildren(node))

    def parseEvents(self):
        """
        Iterates over all the sections in an the parse object and generates the events for each
        Parameters: None
        Returns: None
        """
        for idx, section in enumerate(self.saveSections):
            if (section['type'] == self.TYPE_TABLE):
                for rowIdx, row in enumerate(section['rows']):
                    for columnIdx, cell in enumerate(row[1]):
                        if (len(cell[0]) > 2):
                            self.extract_events_spacy(cell[0], idx, rowIdx, columnIdx)
            else:
                self.extract_events_spacy(str(section["text"]), idx)

    def generateEvent(self, idx, rowIdx, columnIdx, date, startPos, endPos, dText, desc):
        self.linkOffset = 0
        self.currentSection == None
        nodeSection =  { 'section': idx, 'rowIdx': rowIdx, 'columnIdx': columnIdx, 'date': date, 'startPos': startPos, 'endPos': endPos, 'dText': dText, 'desc': desc }
    
        if (nodeSection):
            self.sectionEvents.append(nodeSection)
        print(nodeSection)

    def dep_subtree(self, token, dep):
        deps = [child.dep_ for child in token.children]
        child = next(filter(lambda c: c.dep_ == dep, token.children), None)
        if child != None:
            return " ".join([c.text for c in child.subtree])
        else:
            return ""


    def extract_events_spacy(self, text, idx, rowIdx=None, columnIdx=None):
        """
    	Extracts date time events using the Spacy library.
    	
    	:param text: The text to extract events from.
    	:param idx: The index of the event.
    	:param rowIdx: The row index.
    	:param columnIdx: The column index.
    	"""

        doc = self.nlp(text)
        for ent in filter(lambda e: e.label_ == 'DATE', doc.ents):

            start = parse(ent.text)
            if start == None:
                # could not parse the dates, hence ignore it
                self.generateEvent( idx, rowIdx, columnIdx, None, ent.start_char, ent.end_char, ent.text, None)
                print('Event Discarded: ' + ent.text)
            else:
                self.generateEvent( idx, rowIdx, columnIdx, None, ent.start_char, ent.end_char, ent.text, None)
                # current = ent.root
                # desc = ""
                # while current.dep_ != "ROOT":
                #     current = current.head
                #     desc = " ".join(filter(None, [
                #         self.dep_subtree(current, "nsubj"),
                #         self.dep_subtree(current, "nsubjpass"),
                #         self.dep_subtree(current, "auxpass"),
                #         self.dep_subtree(current, "amod"),
                #         self.dep_subtree(current, "det"),
                #         current.text,
                #         self.dep_subtree(current, "acl"),
                #         self.dep_subtree(current, "dobj"),
                #         self.dep_subtree(current, "attr"),
                #         self.dep_subtree(current, "advmod")]))
                # self.generateEvent(idx, rowIdx, columnIdx, start, ent.start_char, ent.end_char, ent.text, desc)
        return



# with open(abs_file_path) as f:
#     textLines = f.readlines()

# text = ""
# for line in textLines:
#     tempLine = line.replace("\\n", "\n").strip() + " "
#     # replace double escaped characters
#     tempLine = re.sub(r"(\\')", "'", tempLine)
#     text += tempLine



# hToText = html2text.HTML2Text()
# soup = BeautifulSoup(text, features="html.parser")
# temporParse = TemporalParser()
# htmltext = soup.encode('utf-8').decode('utf-8','ignore')
# temporParse.parse(soup)

# temporParse.parseEvents()



# soup.find_all("section")[0].find("table", "infobox")

# for section in soup.find_all("section"):
#     hasInfobox = section.find("table", "infobox")
#     if (hasInfobox):
#         saveSections.append(section)

#     print(article.get_plaintext( skip_categories=False, skip_transclusion=False, skip_headers=False))

# newText  = hToText.handle(htmltext)


#for article in html_dump:
#    if article.title == "189th Infantry Brigade (United States)":
#        print(article.get_plaintext( skip_categories=False, skip_transclusion=False, skip_headers=False))
#        
#    print(article.title)


