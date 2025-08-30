from typing import Any, Self
from dateparser import parse
from bs4 import BeautifulSoup, Tag
from dataclasses import dataclass
import spacy, re

@dataclass
class Formatt:
    section: int
    format: str
    article: str
    start: int
    end: int

@dataclass 
class TableCount:
    column: int = 0
    row: int = 0

@dataclass
class Event:
    section: int
    date: Any
    startPos: int
    endPos: int
    dText: str
    desc: str

@dataclass
class NodeSection:
    type: str
    text: str
    parent_section: Any
    links: list
    events: list
    column_span: int = None
    row_span: int = None
    row: int = None
    column: int = None
    format: str = None

class WikiHtmlParser:
    def __init__(self):
        # if _EXT is added to the end then the section is an extension of the last section
        self.TYPE_MAIN_IMAGE_URL = "MAIN_IMAGE_URL"
        self.TYPE_HEADDING = "HEADING"
        self.TYPE_TITLE = "TITLE"
        self.TYPE_PARAGRAPH = "PARAGRAPH"
        self.TYPE_IMAGE = "IMAGE"
        self.TYPE_TABLE = "TABLE"
        self.TYPE_TABLE_CELL = "TABLE_CELL"
        self.TYPE_QUOTE = "QUOTE"
        self.TYPE_SUBTITLE = "SUBTITLE"
        self.TYPE_SUB_SUBTITLE = "SUB_SUBTITLE"
        self.TYPE_LIST = "LIST"
        self.TYPE_LIST_ITEM = "LIST_ITEM"


        self.p = re.compile(r'\[\d+\]')

        self.nlp = spacy.load("en_core_web_trf")

    def reset_parser(self) -> None:
        self.saveSections : list[NodeSection] = []
        self.sectionFormats : list[Formatt] = []
        self.sectionEvents : list[Event] = []
        # track to number of characters for the current section so we know where the link should be inserted - only tracks for one section
        self.linkOffset : int = 0
        self.nodeText : str = ""
        self.soup : BeautifulSoup = None
        self.currentSection : NodeSection = None
        # variables for allowing table nesting possibly other nestings if required
        self.nestingDepth : int = 0
        self.tableCounts : dict[int, TableCount] = {}

        # set a context for some of the items - depending on what a parent is we might generate different strings
        self.parent_context = None

    def parse(self, soup : BeautifulSoup, title: str) -> None:
        self.reset_parser()
        self.soup = soup
        if (self.soup.find(id="References")):
            self.soup.find(id="References").findParent(name="section").clear()
        if (self.soup.find(id="External_links")):
            self.soup.find(id="External_links").findParent(name="section").clear()
        if (self.soup.find(id="Bibliography")):
            self.soup.find(id="Bibliography").findParent(name="section").clear()
        if (self.soup.find(id="Citations")):
            self.soup.find(id="Citations").findParent(name="section").clear()
        # title is added as the first section
        parent_section = self.generateSection(self.TYPE_HEADDING, None, title)

        self.parseChildren(self.soup.find('body'), parent_section)

    def generateSection(self, type: str, parent_section : NodeSection = None, text: str = None):
        self.linkOffset = 0
        self.nodeText = ""
        self.currentSection == None

        nodeSection = NodeSection(type=type, text=text, parent_section=parent_section, links=[], events=[])

        self.saveSections.append(nodeSection)
        return len(self.saveSections) - 1
    
    def setSectionText(self, sectionIndex : int, text : str):
        self.saveSections[sectionIndex].text = text

    def generateFormatText(self, linkNode : Tag , format: str, parent_section: NodeSection=None) -> str:
        retText = ""
        if (format == 'A'):
            linkText = linkNode.attrs['href']
            # don't include links to files or non existant pages
            if not (linkText.startswith("./File:") or 'redlink=1' in linkText):
                retText = linkNode.text.strip()
            
                self.selectionFormats.append(Formatt(section=len(self.saveSections)-1, format=format,
                                        article=linkText, start=(self.linkOffset),
                                            end=(self.linkOffset + len(retText) ) ) )
        else:
            startText = self.linkOffset
            formatObj = Formatt(len(self.saveSections)-1, format, None, startText, None)
                
            self.sectionFormats.append(formatObj)
            retText = self.parseChildren(linkNode, parent_section)
            formatObj.end = (startText + len(retText))

        return retText
    
    def parseChildren(self, node, parent_section: NodeSection=None) -> str:
        for sectionChild in node.children:
            if len(self.nodeText) > 0 and len(node.text) > 0 and not (self.nodeText[0].isspace() or node.text[-1].isspace()):
                self.nodeText += " "
            childText = self.parseNodes(sectionChild, parent_section)
            self.nodeText += childText
            self.linkOffset = len(self.nodeText)

        if self.nodeText.strip() == "":
            return ""
        self.linkOffset = len(self.nodeText)
        return self.nodeText

    def parseNodes(self,node, parent_section=None) -> str:
        nodeText = ""
        p_section = parent_section
        # print(node.name)
        if (node.name == None):
            if node.text.startswith("<span"):
                return ""
            return node.text.strip()
        if (node.name == "p"):
            self.parent_context = self.TYPE_PARAGRAPH
            self.currentSection = self.TYPE_PARAGRAPH
            new_section = self.generateSection(self.TYPE_PARAGRAPH, p_section)
            nodeText = self.parseChildren(node, new_section)
            
            self.setSectionText(new_section, nodeText)

        elif (node.name == "a"):
            return self.generateFormatText(node, 'a')
        
        elif (node.name == "b" or node.name == "strong"):
            return self.generateFormatText(node, 'b', p_section)
            #return self.parseChildren(node, p_section)
        
        elif (node.name == "i" or node.name == "em"):
            return self.generateFormatText(node, 'i', p_section)
            #return self.parseChildren(node, p_section, " * ", " * ")
            
        elif (node.name == "h2"):
            p_section = self.generateSection(self.TYPE_TITLE, p_section, node.text.strip())

        elif (node.name == "h3"):
            p_section = self.generateSection(self.TYPE_SUBTITLE, p_section, node.text.strip())

        elif (node.name == "h4"):
            p_section = self.generateSection(self.TYPE_SUB_SUBTITLE, p_section, node.text.strip())

        elif (node.name == "section"):
            return self.parseChildren(node, p_section)
            #if nodeText != "":
            #    self.generateSection(self.TYPE_PARAGRAPH, nodeText)
        elif (node.name == "span" or node.name == "abbr" or node.name == "u" or node.name == "div"):
            return self.parseChildren(node, p_section)

        elif (node.name == "ul" or node.name == "ol" or node.name == "dl"):
            p_section = self.generateSection(self.TYPE_LIST, p_section)
            nodeText = self.parseChildren(node, p_section)
            self.setSectionText(p_section, nodeText)
            return ""

        elif (node.name == "li" or node.name == "dt" or node.name == "dd"):
            p_section = self.generateSection(self.TYPE_LIST_ITEM, p_section)
            cell_text = self.parseChildren(node, p_section)
            if (node.text.strip() != ""):
                cell_text = cell_text
            self.saveSections[p_section].text = cell_text
            self.saveSections[p_section].column_span = None
            self.saveSections[p_section].row_span = None
            self.saveSections[p_section].row = None
            self.saveSections[p_section].column = None
            self.saveSections[p_section].format = node.name
        
        elif (node.name == "blockquote"):
            return self.generateFormatText(node, 'blockquote', p_section)
        
        elif (node.name == "table"):
            self.nestingDepth += 1
            self.tableCounts[self.nestingDepth] = TableCount( ) #{ 'column': 0, 'row': 0 }
            p_section = self.generateSection(self.TYPE_TABLE, p_section)
            nodeText = self.parseChildren(node, p_section)
            self.setSectionText(p_section, nodeText)
            self.nestingDepth -= 1
            
        elif (node.name == "thead" or node.name == "tbody"):
            self.parseChildren(node, p_section)

        elif (node.name == "tr"):
            self.parseChildren(node, p_section)
            self.tableCounts[self.nestingDepth].row += 1
            self.tableCounts[self.nestingDepth].column = 0             

        elif (node.name == "th" or node.name == "td"):
            p_section = self.generateSection(self.TYPE_TABLE_CELL, p_section)
            cell_text = self.parseChildren(node, p_section)
            
            column_span = int(node.attrs['colspan']) if 'colspan' in node.attrs and node.attrs['colspan'].isdigit() else 1
            row_span = int(node.attrs['rowspan']) if 'rowspan' in node.attrs and node.attrs['rowspan'].isdigit() else 1
            self.saveSections[p_section].text = cell_text
            self.saveSections[p_section].column_span = column_span
            self.saveSections[p_section].row_span = row_span
            self.saveSections[p_section].row = self.tableCounts[self.nestingDepth].row
            self.saveSections[p_section].column = self.tableCounts[self.nestingDepth].column
            self.saveSections[p_section].format = node.name


            self.tableCounts[self.nestingDepth].column += 1
            
        elif (node.name == "caption"):
            self.caption = self.parseChildren(node, p_section)

        elif (node.name == "img"):
            return ""
        else :
            return ""
        
        return ""

    def parseEvents(self) -> None:
        """
        Iterates over all the sections in an the parse object and generates the events for each
        Parameters: None
        Returns: None
        """
        for idx, section in enumerate(self.saveSections):
            if (len(section["text"]) > 2):
                self.extract_events_spacy(str(section["text"]), idx)

    def generateEvent(self, idx, date, startPos, endPos, dText, desc) -> None:
        self.linkOffset = 0
        self.currentSection == None
        nodeSection = Event(idx, date, startPos, endPos, dText, desc) # { 'section': idx, 'date': date, 'startPos': startPos, 'endPos': endPos, 'dText': dText, 'desc': desc }
    
        if (nodeSection):
            self.sectionEvents.append(nodeSection)
        print(nodeSection)

    def dep_subtree(self, token, dep) -> str:
        deps = [child.dep_ for child in token.children]
        child = next(filter(lambda c: c.dep_ == dep, token.children), None)
        if child != None:
            return " ".join([c.text for c in child.subtree])
        else:
            return ""


    def extract_events_spacy(self, text, idx, time_parser) -> None:
        """
    	Extracts date time events using the Spacy library.
    	
    	:param text: The text to extract events from.
    	:param idx: The section of the article.
    	:param rowIdx: The row index.
    	:param columnIdx: The column index.
    	"""
        parsed_date = None
        doc = self.nlp(text)
        for ent in filter(lambda e: e.label_ == 'DATE' or e.label_ == 'TIME', doc.ents):
            parsed_date = self.parse_date(ent.text, parsed_date, time_parser)
            if parsed_date == None:
                self.generateEvent( idx, None, ent.start_char, ent.end_char, ent.text, None)
            else:
                current = ent.root
                desc = ""
                while current.dep_ != "ROOT":
                    current = current.head
                    desc = " ".join(filter(None, [
                        self.dep_subtree(current, "nsubj"),
                        self.dep_subtree(current, "nsubjpass"),
                        self.dep_subtree(current, "auxpass"),
                        self.dep_subtree(current, "amod"),
                        self.dep_subtree(current, "det"),
                        current.text,
                        self.dep_subtree(current, "acl"),
                        self.dep_subtree(current, "dobj"),
                        self.dep_subtree(current, "attr"),
                        self.dep_subtree(current, "advmod")]))
                self.generateEvent( idx, parsed_date, ent.start_char, ent.end_char, ent.text, desc)
        return
    
    def parse_date(self, date_text, reference_date, time_parser):
        try:
            if reference_date:
                time_parser.set_refrence_date(reference_date.to_datetime('start'), reference_date.grain)
            date = time_parser.parse(date_text)

            print("result: " + str(date))
            return date

        except Exception as e:
            #msg = str(e)
            #if (hasattr(e, 'msg')):
            print(str(e), " ", date_text)
            #else:
            #    raise (e)


