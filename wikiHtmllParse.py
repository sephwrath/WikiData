from dateparser import parse
from bs4 import BeautifulSoup
import spacy, re

class WikiHtmlParser:
    def __init__(self):
        # if _EXT is added to the end then the section is an extension of the last section
        self.TYPE_MAIN_TITLE = "MAIN_TITLE"
        self.TYPE_MAIN_IMAGE_URL = "MAIN_IMAGE_URL"
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
        self.LIST_TYPE_BULLETED = "BULLETED"
        self.LIST_TYPE_NUMBERED = "NUMBERED"
        self.LIST_TYPE_INDENTED = "INDENTED"


        self.p = re.compile(r'\[\d+\]')

        self.nlp = spacy.load("en_core_web_trf")

    def reset_parser(self):
        self.saveSections = []
        self.sectionLinks = []
        self.sectionEvents = []
        # track to number of characters for the current section so we know where the link should be inserted - only tracks for one section
        self.linkOffset = 0
        self.soup = None
        self.currentSection = None
        # variables for allowing table nesting possibly other nestings if required
        self.nestingDepth = 0
        self.tableCounts = {}

        # set a context for some of the items - depending on what a parent is we might generate different strings
        self.parent_context = None

    def parse(self, soup : BeautifulSoup, title: str):
        self.reset_parser()
        self.soup = soup
        if (self.soup.find(id="References")):
            self.soup.find(id="References").findParent(name="section").clear()
        if (self.soup.find(id="External_links")):
            self.soup.find(id="External_links").findParent(name="section").clear()
        # title is added as the first section
        parent_section = self.generateSection(self.TYPE_TITLE, None, title)

        self.parseChildren(self.soup.find('body'), parent_section, 0)

    def generateSection(self, type, parent_section = None, text=None):
        self.linkOffset = 0
        self.currentSection == None

        nodeSection =  { 'type': type, 'text': text, 'parent_section': parent_section, 'links': [], 'events': [] }

        self.saveSections.append(nodeSection)
        return len(self.saveSections) - 1
    
    def setSectionText(self, sectionIndex, text):
        self.saveSections[sectionIndex]['text'] = text

    def generateLinkText(self, linkNode):
        strippedText = linkNode.text.strip()

        linkText = linkNode.attrs['href']
        # don't include links to files or non existant pages
        if not (linkText.startswith("./File:") or 'redlink=1' in linkText):
        
            if (self.parent_context == self.TYPE_TABLE):
                self.sectionLinks.append({ 'section': len(self.saveSections)-1,
                                   'article': linkText, 'start': (self.linkOffset + 1),
                                    'end': (self.linkOffset + len(strippedText) + 1),
                                    'column': len(self.currentRow), 'row': len(self.tableRows) } )
            else:
                self.sectionLinks.append({ 'section': len(self.saveSections)-1,
                                   'article': linkText, 'start': (self.linkOffset + 1),
                                    'end': (self.linkOffset + len(strippedText) + 1) } )

        return strippedText
    
    def parseChildren(self, node, parent_section=None, leading=None, trailing=None):
        nodeText = ""
        for sectionChild in node.children:
            childText = self.parseNodes(sectionChild, parent_section)
            if len(nodeText) > 0 and len(childText) > 0 and not (nodeText[0].isspace() or childText[-1].isspace()):
                nodeText += " "
            nodeText += childText
            self.linkOffset = len(nodeText)

        if nodeText.strip() == "":
            return ""
        if leading :
            nodeText = leading + nodeText
        if trailing:
            nodeText = nodeText + trailing
        self.linkOffset = len(nodeText)
        return nodeText

    def parseNodes(self,node, parent_section=None):
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
            return self.generateLinkText(node)
        
        elif (node.name == "b" or node.name == "strong"):
            return self.parseChildren(node, p_section," ** ", " ** ")
        
        elif (node.name == "i" or node.name == "em"):
            return self.parseChildren(node, p_section, " * ", " * ")
            
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
            return self.parseChildren(node, p_section, " - ", "\n")
        
        elif (node.name == "blockquote"):
            return self.parseChildren(node, p_section, " > ")
        
        elif (node.name == "table"):
            self.nestingDepth += 1
            self.tableCounts[self.nestingDepth] = { 'column': 0, 'row': 0 }
            p_section = self.generateSection(self.TYPE_TABLE, p_section)
            nodeText = self.parseChildren(node, p_section)
            self.setSectionText(p_section, nodeText)
            self.nestingDepth -= 1
            
        elif (node.name == "thead" or node.name == "tbody"):
            self.parseChildren(node, p_section)

        elif (node.name == "tr"):
            self.parseChildren(node, p_section)
            self.tableCounts[self.nestingDepth]['row'] += 1
            self.tableCounts[self.nestingDepth]['column'] = 0             

        elif (node.name == "th" or node.name == "td"):
            p_section = self.generateSection(self.TYPE_TABLE_CELL, p_section)
            cell_text = self.parseChildren(node, p_section)
            
            column_span = int(node.attrs['colspan']) if 'colspan' in node.attrs and node.attrs['colspan'].isdigit() else 1
            row_span = int(node.attrs['rowspan']) if 'rowspan' in node.attrs and node.attrs['rowspan'].isdigit() else 1
            self.saveSections[p_section]['text'] = cell_text
            self.saveSections[p_section]['column_span'] = column_span
            self.saveSections[p_section]['row_span'] = row_span
            self.saveSections[p_section]['row'] = self.tableCounts[self.nestingDepth]['row']
            self.saveSections[p_section]['column'] = self.tableCounts[self.nestingDepth]['column']
            self.saveSections[p_section]['type'] = node.name


            self.tableCounts[self.nestingDepth]['column'] += 1
            
        elif (node.name == "caption"):
            self.caption = self.parseChildren(node, p_section)

        elif (node.name == "img"):
            return ""
        else :
            return ""
        
        return ""

    

    def parseEvents(self):
        """
        Iterates over all the sections in an the parse object and generates the events for each
        Parameters: None
        Returns: None
        """
        for idx, section in enumerate(self.saveSections):
            if (len(section["text"]) > 2):
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
    	:param idx: The section of the article.
    	:param rowIdx: The row index.
    	:param columnIdx: The column index.
    	"""

        doc = self.nlp(text)
        for ent in filter(lambda e: e.label_ == 'DATE' or e.label_ == 'TIME', doc.ents):
            self.generateEvent( idx, rowIdx, columnIdx, None, ent.start_char, ent.end_char, ent.text, None)

            #start = parse(ent.text)
            #if start == None:
                # could not parse the dates, hence ignore it
            #    self.generateEvent( idx, rowIdx, columnIdx, None, ent.start_char, ent.end_char, ent.text, None)
                #print('Event Discarded: ' + ent.text)
            #else:
            #    self.generateEvent( idx, rowIdx, columnIdx, None, ent.start_char, ent.end_char, ent.text, None)
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


