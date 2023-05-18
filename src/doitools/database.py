'''
Created on 9 Aug 2018

@author: ostlerr
'''
import sys
import pyodbc
import json
from datetime import date
import configparser
import datacite
from dataCiteConnect import getDataCiteClient 
from datacite.schema41 import contributors, creators, descriptions, dates, sizes,\
    geolocations, fundingreferences, related_identifiers
from datacite import schema41

class DocumentInfo:
    
    def __init__(self):
        self.url = None
        self.mdId = None
        self.data = None
        self.isExternal = None
        self.DOI = None
        
        
class Person:
    def __init__(self, row):
        self.familyName = row.family_name
        self.givenName = row.given_name 
        self.nameIdentifier = row.name_identifier 
        self.nameIdentifierScheme = row.name_identifier_scheme 
        self.schemeUri = row.scheme_uri 
        self.organisationName = row.name 
        self.street = row.street_address
        self.locality = row.address_locality 
        self.region = row.address_region 
        self.country = row.address_country 
        self.postalCode = row.postal_code
        self.fullname = self.givenName + " " + self.familyName
        if hasattr(row,'contributor_type'):
            self.contributorType = row.contributor_type
        else:
            self.contributorType = "Researcher"
        self.nameIdentifiers = None
        if not self.nameIdentifier is None:
            self.nameIdentifiers = [
                {
                    "nameIdentifier": self.nameIdentifier,
                    "nameIdentifierScheme": self.nameIdentifierScheme,
                    "schemeURI": self.schemeUri 
                }
            ]
        
        self.affiliations = [self.formatAddress()]
        
    def formatAddress(self):
        address = self.organisationName
        if not self.street is None:
            address = address + ", " + self.street
        if not self.locality is None:
            address = address + ", " + self.locality
        if not self.region is None:
            address = address + ", " + self.region
        if not self.postalCode is None:
            address = address + ", " + self.postalCode
        if not self.country is None:
            address = address + ", " + self.country
        return address
#        
    def asCreatorJson(self):
        creator = dict(creatorName = self.fullname,givenName = self.givenName,familyName = self.familyName)
        if not self.nameIdentifiers is None:                
            creator["nameIdentifiers"] = self.nameIdentifiers
            creator["affiliations"] = self.affiliations
        else:
            creator["passed"] = 'false'
        return creator
    
    def asContributorJson(self):
        contributor = dict(contributorType = self.contributorType, contributorName = self.fullname, givenName = self.givenName, familyName = self.familyName)
        if not self.nameIdentifiers is None:                
            contributor["nameIdentifiers"] = self.nameIdentifiers
        contributor["affiliations"] = self.affiliations
        return contributor

def connect():
    
    config = configparser.ConfigParser()
    config.read('config.ini')
    dsn = config['SQL_SERVER']['DSN']
    uid = config['SQL_SERVER']['UID']
    pwd = config['SQL_SERVER']['PWD']
    con = pyodbc.connect('DSN='+dsn+';uid='+uid+';pwd='+pwd)
    #con = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=Z:\website development\datacite\DataCite Metadata database.accdb;')
    #con = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:\code\access\DataCite Metadata database.accdb;')
    return con

def getCursor():
    con = connect()
    cur = con.cursor()
    return cur

def getDocumentMetadata(mdId):
    cur = getCursor()
    cur.execute("""select m.id, m.url, m.identifier, m.identifier_type, m.title, m.publication_year, m.lang, m.version,
m.description_quality,m.description_provenance,m.description_other, m.description_abstract,m.description_methods,m.description_toc,m.description_technical_info,
m.is_external, m.is_ready , 
o.name as publisher,  
grt.type_value grt_value, srt.type_value  srt_value,
df.mime_type , df.id , m.rights_text, m.rights_licence_uri, m.rights_licence, 
fl.name, fl.geo_point_latitude, fl.geo_point_longitude
        from metadata_documents m
        inner join organisations o on m.publisher_id = o.id
        inner join general_resource_types grt on m.general_resource_type_id  = grt.id
        left outer join specific_resource_types srt on m.specific_resource_type_id  = srt.id
        inner join document_formats df  on m.document_format_id  = df.id
        inner join experiments lte on m.experiment_id  = lte.id
        inner join fields fl on lte.field_id = fl.id
        where m.id = ?""", mdId)
    return cur

def prepareCreators(mdId):
    cur = getCursor()
    creators = []
    # First prepare named people
    cur.execute('''select p.family_name, p.given_name, p.name_identifier, p.name_identifier_scheme, p.scheme_uri, 
o.name, o.street_address, o.address_locality, o.address_region, o.address_country, 
o.postal_code 
from persons p 
inner join person_creators pc on p.id = pc.person_id 
inner join organisations o on p.organisation_id  = o.id where pc.metadata_document_id = ?''', mdId)
    
    results = cur.fetchall()    
    for row in results: 
        person = Person(row)        
        creators.append(person.asCreatorJson())
           
    # second prepare organisations
    cur.execute("""select name 
from organisations o 
inner join organisation_creators oc on o.id = oc.organisation_id where oc.metadata_document_id = ?""",mdId)
    results = cur.fetchall()
    for row in results:
        creators.append({"creatorName": row.name}) 
        
    return creators

def prepareContributors(mdId):
    cur = getCursor()
    contributors = [] 
    # First prepare named people
    cur.execute("""select p.family_name, p.given_name, p.name_identifier, p.name_identifier_scheme, p.scheme_uri,
o.name, o.street_address, o.address_locality, o.address_region, o.address_country, o.postal_code, 
prt.type_value contributor_type
        from persons p 
        inner join organisations o on p.organisation_id  = o.id
        inner join person_roles pr on p.id = pr.person_id
        inner join person_role_types prt on pr.person_role_type_id  = prt.id
        where pr.metadata_document_id = ?""", mdId)
    
    results = cur.fetchall()    
    for row in results: 
        person = Person(row)        
        contributors.append(person.asContributorJson())
        # contributors.append({"contributorName": row.name}) 
        
    # second prepare organisations
    # nathalie 2023-05-17: removed as we do not have a table organisation_roles in gilbert
    # cur.execute("""select o.organisation_name, ort.type_value 
    #     from (organisation o 
    #     inner join organisation_role r on o.organisation_id = r.organisation_id) 
    #     inner join organisation_role_types ort on r.ort_id = ort.ort_id
    #     where r.md_id = ?""",mdId)
    # results = cur.fetchall()
    # for row in results:
    #     contributors.append({"contributorName": row.organisation_name}) 
        
    return contributors    
    
def prepareSubjects(mdId):
    cur = getCursor()
    subjects = []
    cur.execute("""select s.subject, s.uri s_uri, ss.name  , ss.uri ss_uri 
        from subjects s
        inner join subject_schemas ss on s.subject_schemas_id  = ss.id
        inner join document_subjects ds on s.id  = ds.subject_id 
        where ds.metadata_document_id  = ?""", mdId)
    results = cur.fetchall()    
    for row in results: 
        subjects.append({'lang' : 'en', 'subjectScheme' : row.name, 'schemeURI' : row.ss_uri, 'valueURI' : row.s_uri, 'subject' : row.subject})
        
    return subjects
    
def prepareDescriptions(row):
    descriptions = []
    
    descriptions.append({'lang' : row.lang, 'descriptionType' : 'Abstract', 'description' : row.description_abstract})
    if not row.description_methods is None:
        descriptions.append({'lang' : row.lang, 'descriptionType' : 'Methods', 'description' : row.description_methods})
    if not row.description_toc is None:
        descriptions.append({'lang' : row.lang, 'descriptionType' : 'TableOfContents', 'description' : row.description_toc})
    if not row.description_technical_info is None:
        descriptions.append({'lang' : row.lang, 'descriptionType' : 'TechnicalInfo', 'description' : row.description_technical_info})
    if not row.description_quality is None or not row.description_provenance is None or not row.description_other is None:
        descriptions.append({'lang' : row.lang, 'descriptionType' : 'Other', 'description' : str(row.description_provenance) + " " + str(row.description_quality) + " " + str(row.description_other)})
    
    return descriptions

def prepareDates(mdId):
    cur = getCursor()
    dates = []
    cur.execute("""select dt.type_value, dd.document_date 
from document_dates dd inner join date_types dt on dd.date_type_id  = dt.id  where dd.metadata_document_id  = ?""", mdId)
    
    results = cur.fetchall()    
    for row in results: 
        dates.append({'date': row.document_date.strftime('%Y-%m-%d'),'dateType' : row.type_value})
        
    return dates
    
def prepareRelatedIdentifiers(mdId):
    cur = getCursor()
    related_identifiers = []
    cur.execute("""select ri.identifier, i.id as identifier_type, r.type_value as relation_type
        from related_identifiers ri
        inner join identifier_types i on ri.identifier_type_id  = i.id
        inner join relation_types r on ri.relation_type_id  = r.id
        where ri.metadata_document_id  = ?""", mdId)
    
    results = cur.fetchall()    
    for row in results: 
        related_identifiers.append({'relatedIdentifier': row.identifier,'relatedIdentifierType' : row.identifier_type, 'relationType' : row.relation_type})
        
    return related_identifiers    

def prepareSizes(mdId):
    cur = getCursor()
    sizes = []
    cur.execute("""select du.name  , df.size_value
        from document_files df  inner join document_units du  on df.document_unit_id  = du.id  
        where is_illustration  = 0 and df.metadata_document_id  = ?""", mdId)
    
    results = cur.fetchall()    
    for row in results: 
        if row.name == 'None':
            sizes.append(row.size_value)
        else:
            sizes.append(str(row.size_value) + ' ' + row.name)
        
    return sizes

def prepareFundingReferences(mdId):
    cur = getCursor()
    fundingreferences = []
    cur.execute("""select fa.reference_number , fa.uri , fa.title ,o.name , o.funder_identifier , o.funder_identifier_type 
        from document_funders  df
        inner join funding_awards fa on df.funding_award_id  = fa.id
        inner join organisations o  on fa.organisation_id = o.id
        where df.metadata_document_id  = ?""", mdId)

    results = cur.fetchall()
    for row in results:
        fundingreferences.append(
        {
            "funderName": row.name,
            "funderIdentifier": {
                "funderIdentifier": row.funder_identifier,
                "funderIdentifierType": row.funder_identifier_type
            },
            "awardNumber": {
                "awardNumber": row.reference_number,
                "awardURI": row.uri
            },
            "awardTitle": row.title
        })
        
    return fundingreferences

def process(documentInfo):
    mdId = documentInfo.mdId
    mdCursor = getDocumentMetadata(mdId)
    mdRow = mdCursor.fetchone()
    data = None
    if mdRow:
        mdUrl = mdRow.url
        documentInfo.url = mdUrl
        documentInfo.DOI = mdRow.identifier
        data = {
            'identifier' : {
                'identifier' : mdRow.identifier,
                'identifierType' : 'DOI'
            },
            'creators' : prepareCreators(mdId),
            'titles' : [
                {'title' : mdRow.title}
            ],
            'publisher' : mdRow.publisher,
            'publicationYear' : mdRow.publication_year,
            'resourceType': {'resourceTypeGeneral' : mdRow.grt_value},
            'subjects' : prepareSubjects(mdId),
            'contributors' : prepareContributors(mdId),
            'dates' : prepareDates(mdId),
            'language' : mdRow.lang,        
            'version' : str(mdRow.version),
            'relatedIdentifiers' : prepareRelatedIdentifiers(mdId),
            'sizes' : prepareSizes(mdId),
            'formats' : [mdRow.mime_type],
            'rightsList' : [
                {'rightsURI' : mdRow.rights_licence_uri, 'rights' : mdRow.rights_licence},
                {'rights' : mdRow.rights_text}
            ],
            'descriptions' : prepareDescriptions(mdRow),
            'geoLocations': [
                {
                    'geoLocationPoint' : {
                        'pointLongitude': float(mdRow.geo_point_longitude),
                        'pointLatitude': float(mdRow.geo_point_latitude)
                    },
                    'geoLocationPlace': mdRow.name            
                }
            ],
            'fundingReferences' : prepareFundingReferences(mdId)
        }
        
        
    documentInfo.isExternal =  mdRow.is_external if mdRow.is_external else 0
    documentInfo.data = data
    strJsData =  json.dumps(data, indent=4)
    ##print("database.process.line311. = data" +strJsData)
    return documentInfo    

def logDoiMinted(documentInfo):
    try:
        con = connect()
        cur = con.cursor()
        cur.execute("update metadata_documents set doi_created = getdate() where id = ?", documentInfo.mdId)
        con.commit()
    except AttributeError as error:
        print(error)
    except pyodbc.Error as error:
        print(error)


        
    
if __name__ == '__main__':
    try: 
        documentInfo = DocumentInfo()        
        documentInfo.mdId = input('Enter Document ID: ')
        documentInfo = process(documentInfo)
        externalDS = documentInfo.isExternal
        DOIURL = "https://doi.org/"+documentInfo.DOI
        
        if externalDS == 0:
            xname = "D:/doi_out/"+ str(documentInfo.mdId) + ".xml"
            fxname = open(xname,'w+')
            fxname.write(schema41.tostring(documentInfo.data))
            fxname.close()
            d = getDataCiteClient()
            d.metadata_post(schema41.tostring(documentInfo.data))
            doi = documentInfo.data['identifier']['identifier']
            d.doi_post(doi, documentInfo.url)
            logDoiMinted(documentInfo)
            docID =  documentInfo.mdId
            print ("update metadata_document set doi_created = getdate() where md_id ="+docID)
            print ("xml file saved in " + xname)
            print (DOIURL)
            print('done')
        else: 
            print("external dataset not minted")
    except datacite.errors.DataCiteServerError as error:
        print(error)
    except:
        print("Unexpected error:", sys.exc_info())        
        
    
