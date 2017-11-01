import xml.etree.ElementTree as ET
import sqlite3
import argparse
from datetime import datetime
import dateutil.parser
import pytz

class Revision:
    def __init__(self):
        self.timestamp=datetime(1,1,1,tzinfo=pytz.utc)
        self.status=None
        self.id=-1
        self.author=""

class Page:
    def __init__(self):
        self.title=""
        self.namespace=""
        self.root=""
        self.text=""
        self.id=-1
        #self.status=-1
        self.comment=""
        self.revisions=[]
        self.lastStatusChange=datetime(1,1,1,tzinfo=pytz.utc)

class Work:
    def __init__(self):
        self.title=""
        self.pages=[]
        self.pagesStatus=[0,0,0,0,0]
        self.id=-1
        self.numberPages=0
        self.indexStatus="no index"
        self.lastChange=datetime(1,1,1,tzinfo=pytz.utc)
        self.lastChangedPage=""

indexStatuses={
        'C':"To be proofread",
        'X':"Pagelist missing",
        'R':"",
        'OCR':"Source file needs an OCR text layer",
        'L':"Source file is incorrect",
        'T':"Validated",
        'V':"Proofread",
        'MS':"Ready for match and split"}

def create_tables(cursor):
    cursor.execute('''DROP TABLE IF EXISTS project;''')
    cursor.execute('''DROP TABLE IF EXISTS page;''')
    cursor.execute('''DROP TABLE IF EXISTS revision;''')
    cursor.execute('''CREATE TABLE project(id INTEGER, name TEXT, status TEXT,nbpages INTEGER)''')
    cursor.execute('''CREATE TABLE page(id INTEGER, name TEXT, projectname TEXT, lastchange TEXT, status TEXT)''')
    cursor.execute('''CREATE TABLE revision(num INTEGER, pageid INTEGER, author TEXT, comment TEXT, time TIMESTAMP,oldstatus INTEGER,status INTEGER)''')

def parse_dumpfile(dump,db):
    cursor = db.cursor()
    dump_iter = iter(dump)

    event,root=next(dump_iter)

    #Dump parsing:
    #The whole dump is a single large XML file: there is one elem <page> for each page, then inside it one elem <revision> for each revision

    #We keep track of the parents of the current element, in order to know if we are inside a revision or a page
    parent=[]

    #For efficiency, elements are inserted in bulk in the database, so are first stored in these arrays:
    page_records=[]
    revision_records=[]
    project_records=[]
    item_index = 0
    item_group_size = 100

    #Insertion queries
    insert_project_query='INSERT INTO project (id,name,status,nbpages) VALUES (?,?,?,?)'
    insert_page_query='INSERT INTO page (id,name,projectname,lastchange,status) VALUES (?,?,?,?,?)'
    insert_revision_query='INSERT INTO revision (num,pageid,author,comment,time,oldstatus,status) VALUES (?,?,?,?,?,?,?)'

    for event,elem in dump_iter:
        tag=elem.tag.split('}')[1]
        if event=="start":
            parent.append(tag)
        if event=="end":
            if len(parent)>0:
                parent.pop()
        
        if event=="start":
            #We start a new element and initialize the corresponding object
            if tag=='page':
                p=Page()
            if tag=='revision': 
                r=Revision()

        #We store the tags inside a revision
        if event == "end" and len(parent)>0 and parent[-1] == "revision":
            if tag == 'id':
                r.id=int(elem.text)
            elif tag == 'username':
                r.author=elem.text
            elif tag=='timestamp':
                r.timestamp=dateutil.parser.parse(elem.text)
            elif tag == 'text':
                if elem.text is not None:
                    if elem.text[0:31]=='<noinclude><pagequality level="':
                        r.status=int(elem.text[31:32])
                    #we also try the obsolete way of doing things:
                    elif elem.text[0:25]=='<noinclude>{{PageQuality|':
                        r.status=int(elem.text[25:26])
                    else:
                        r.status=None

        #We store the tags inside a page
        if event=="end" and len(parent)>0 and parent[-1]=="page":
            if tag == 'id':
                p.id = int(elem.text)
            elif tag == 'revision':
                p.revisions.append(r)
                elem.clear()
            elif tag == 'ns':
                p.namespace = elem.text
            elif tag == 'comment':
                p.comment = elem.text
            elif tag == 'title':
                p.title=elem.text
                #The work the page belongs to is deducted from the page title
                p.work=elem.text.split('/')[0]
                if p.work[0:5]=='Page:':
                    p.work=p.work[5:]
                if p.work[0:6]=='Index:':
                    p.work=p.work[6:]
            elif tag == 'text':
                p.text = elem.text

        #We reach the end of a page:
        if tag == 'page' and event == 'end':
           
            #We skip redirects
            if p.text[0:9].lower()=='#redirect':
                continue
            
            #Index pages:
            if p.namespace=='106':
                pos=p.text.find("|Progress=") 
                if pos==-1:
                    status="No status"
                else:
                    subtext=p.text[pos+10:pos+23]
                    pos2=subtext.find('|')
                    status=subtext[:pos2].strip()
                pages=p.text.find("|Pages=")
                print(p.title)
                print(p.text[pages:pages+100])
                project=p.title.split(':')[1]
                indexStatus=indexStatuses.get(status,status)
                project_record=(p.id,project,indexStatus,0)
                project_records.append(project_record)

            #This is a Page page         
            if p.namespace=='104':
                item_index = item_index + 1
                status=-1
                for r in p.revisions:
                    if r.status!=status:
                        #The status was changed for this revision:
                        p.lastStatusChange=r.timestamp
                        revision_record=(r.id,p.id,r.author,"",r.timestamp,status,r.status)
                        revision_records.append(revision_record)
                        status=r.status
                page_record=(p.id,p.title,p.work,p.lastStatusChange,status)
                page_records.append(page_record)
            elem.clear()
            root.clear()
            if item_index % item_group_size==0:
                if len(project_records)>0:
                    cursor.executemany(insert_project_query,project_records)
                if len(page_records)>0:
                    cursor.executemany(insert_page_query,page_records)
                if len(revision_records)>0:
                    cursor.executemany(insert_revision_query,revision_records)
                db.commit()
                page_records=[]
                revision_records=[]
                project_records=[]
    if len(page_records)>0:
        cursor.executemany(insert_page_query,page_records)
    if len(revision_records)>0:
        cursor.executemany(insert_revision_query,revision_records)
    if len(project_records)>0:
        cursor.executemany(insert_project_query,project_records)


def initialize(dumpfile,database):
    dump = ET.iterparse(dumpfile,events=("start","end"))


    db = sqlite3.connect(database)
    cursor = db.cursor()
    create_tables(cursor)
    
    parse_dumpfile(dump,db)

            


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description='Initialize the database from a dump file.')
    parser.add_argument('dumpfile',help='the Wikisource dump file')
    parser.add_argument('database',help='the SQLite file to store the parsed data')
    args = parser.parse_args()
    initialize(args.dumpfile,args.database)
