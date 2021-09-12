import pyodbc
import sys
import pandas as pd
import json
from arcpy import FromWKT, AsShape
from ui_functions import Debug
import time
from datetime import datetime

#import shapely
#from shapely.geometry import shape
#import geojson

def RemoveNulls(dict_in):
    #returns dictionary with only non-null entries
    dict_in = {k: v for k, v in dict_in.items() if v is not None}

    return dict_in

def CleanDeltas(dict_in):
    #remove nulls, turn all keys to lower case
    dict_in = {k.lower(): v for k, v in dict_in.items() if v is not None}

    return dict_in

def AddQuotes(dict_in):
    #adds quote marks to non-float values, turns all values into strings, escapes apostrophes
    
    if (not isinstance(dict_in[k], float)) and (not isinstance(dict_in[k], int)):
        dict_in[k] = str(dict_in[k]).replace("'", "''")
        dict_in[k] = "'{}'".format(dict_in[k])
    else:
        dict_in[k] = str(dict_in[k])
        
    return dict_in

def LowercaseDataframe(df):
    #converts all column names to lower case
    df.columns = [col.lower() for col in df.columns]

    return df

def Connect(server, database, UID, PWD):
    Debug('Connecting to SQL Server...', 2)
    
    connection_string = 'Driver={{SQL Server}};Server={};Database={};User Id={};Password={}'.format(server, database, UID, PWD)

    Debug('SQL Connection string: "{}"\n'.format(connection_string), 3, indent=4)
    
    try:
        connection = pyodbc.connect(connection_string)
        
    except:
        Debug("Connection error: {}".format(sys.exc_info()[0]), 1, indent=4)
        return False

    Debug('Connected!\n', 2, indent=4)
    return connection

def ReadSQLWithDebug(query, connection):
    Debug('SQL Query: "{}"\n'.format(query), 0)
    df = pd.read_sql(query, connection)
    df = LowercaseDataframe(df)
    return df

def CheckFeatureclass(connection, fcName):
    #Checks that featureclass has globalids and is registered as versioned
    
    Debug('Checking "{}"...'.format(fcName), 2)
    
    query = "SELECT registration_id FROM SDE_table_registry WHERE table_name = '{}'".format(fcName)
    data = ReadSQLWithDebug(query, connection)
    
    if (len(data.index) < 1):
        Debug("'{}' not found in SDE table registry. Check that it has been registered as versioned.\n".format(fcName), 1)
        return False

    query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' AND COLUMN_NAME = 'GLOBALID'".format(fcName)
    data = ReadSQLWithDebug(query, connection)
    
    if (len(data.index) < 1):
        Debug('Featureclass has no global IDs!', 1)
        return False

    Debug('Featureclass is valid.\n', 1, indent=4)
    return True   

def GetCurrentStateId(connection):
    #returns current state id of DEFAULT version
    Debug('Getting current SDE state id...', 2)
    
    query = "SELECT state_id FROM SDE_versions WHERE NAME='DEFAULT'" #TODO: allow for other versions?
    response = ReadSQLWithDebug(query, connection)

    try:
        state_id = response.iloc[0, 0]
    except:
        print('Fatal error! Could not aquire current state id.\n')
        exit()

    print(state_id)

    Debug('SDE state id: {}\n'.format(state_id), 2, indent=4)
    return int(state_id)

def GetDatatypes(connection, fcName):
    #grabs column datatypes from featureclass

    query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'".format(fcName)
    response = ReadSQLWithDebug(query, connection)

    print(response)
    return response

def GetSRID(connection, fcName):
    #gets SRID of featureclass

    Debug('Getting SRID...', 2)

    try:
        query = "SELECT TOP 1 SHAPE.STSrid FROM {}_evw".format(fcName)
        response = ReadSQLWithDebug(query, connection)
        srid = int(response.iloc[0])
        Debug('Done.\n', 2, indent=4)
    except:
        Debug('Error getting SRID! Defaulting to 26910.', 2, indent=4)
        srid = 26910

    return srid

def GetGlobalIds(connection, fcName):
    #returns list of global ids existing in featureclass
    Debug('Getting SDE Global IDs...', 2)

    query = "SELECT GLOBALID FROM {}_evw".format(fcName)
    globalIds = ReadSQLWithDebug(query, connection)

    globalIdsList = globalIds.iloc[:, 0].tolist()

    print(globalIdsList)

    Debug('Done.\n', 2, indent=4)

    return globalIdsList

def GetChanges(connection, fcName, stateId):
    #returns rows from versioned view with state id > state

    Debug('Getting SDE changes...', 1)

    currentStateId = GetCurrentStateId(connection)

    #get rows from adds table since lastState
    query = "SELECT * FROM {}_evw WHERE SDE_STATE_ID > {} AND SDE_STATE_ID <= {}".format(fcName, stateId, currentStateId)
    adds = ReadSQLWithDebug(query, connection)

    if(len(adds.index) > 0 and 'shape' in adds.columns):
        #reaquire SHAPE column as WKT
        query = "SELECT SHAPE.STAsText() FROM {}_evw WHERE SDE_STATE_ID > {} AND SDE_STATE_ID <= {}".format(fcName, stateId, currentStateId)
        shape = ReadSQLWithDebug(query, connection)

        #replace shape column with text
        adds['shape'] = shape.values

    return adds

def WkbToEsri(WKB):
    #converts well known binary to esri json
    Debug('Converting WKB to Esri Json...\n', 3)
    Debug('WKB: {}\n'.format(WKB), 3, indent=4)
    
    geom = FromWKT(WKB)
    esri = geom.JSON
    
    Debug('Converted Esri Json: {}\n'.format(esri), 3, indent=4)
    
    return json.loads(esri)

def EsriToWkb(jsn):
    #converts esri json to well known text

    Debug('Converting Esri Json to WKT...\n', 3)

    try:
        srid = jsn['spatialReference']['wkid']
    except:
        print('No wkid found! Defaulting to 26910.')
        srid = 26910
    
    jsn = json.dumps(jsn)
    
    Debug('Esri Json: {}\n'.format(jsn), 3, indent=4)
          
    geom = AsShape(jsn, True)
    #p = shapely.wkt.loads(wkt_text)
    #from shapely import wkb
    #return (wkb.dumps(p, hex=true))
    #print(geom.WKB)
    wkt = geom.WKT

    Debug('Converted WKT: {}\n'.format(wkt), 3, indent=4)

    sql = "geometry::STGeomFromText('{}', {})".format(wkt, srid)
    
    return sql

def GetDatetimeColumns(datatypes):
    datetime_columns = (datatypes[datatypes['data_type'].str.contains('datetime')])['column_name'].tolist()
    datetime_columns = [col.lower() for col in datetime_columns]

    return datetime_columns

def SqlDatetimeToEpoch(string):
    string = string.split('.')[0]
    utc_time = datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
    return (utc_time - datetime(1970, 1, 1)).total_seconds()*1000
    
def SqlToJson(df, datatypes):
    #takes adds or updates dataframe and converts into agol-json-like dictionary
    dict_out = []

    #attributes = json.loads(df.to_json(orient='index'))
    #print(json.dumps(attributes, indent=4))

    #separate shape column from dataframe
    #shapes = df['SHAPE']
    #df = df.drop(labels='SHAPE', axis='columns')

    #get columns containing datetime objects
    datetime_columns = GetDatetimeColumns(datatypes)
    
    for i in range(0, len(df.index)):
        attributes = df.iloc[i]
        attributes = json.loads(attributes.to_json(orient='index'))

        #remove nulls, convert keys to lower case
        attributes = CleanDeltas(attributes)

        #separate out shape
        if ('shape' in attributes.keys()): 
            geometry = WkbToEsri(attributes['shape'])
            del attributes['shape']
        else:
            print('No shape')

        #convert datetime strings to epoch timestamps
        for k in attributes.keys():
            if k in datetime_columns:
                epoch = SqlDatetimeToEpoch(attributes[k])
                print str(epoch)
                attributes[k] = epoch

        #print(attributes)
        entry = {'geometry': geometry, 'attributes': attributes}
        print(json.dumps(entry))
        dict_out.append(entry)

    return dict_out

def JsonToSql(deltas, datatypes):
    #takes adds or updates json and turns it into sql-writable format
    dict_out = []

    #get datetime columns (need to be converted to epoch)
    datetime_columns = GetDatetimeColumns(datatypes)
    
    for delta in deltas:
        #turn geometry json into syntax for SQL
        SHAPE = EsriToWkb(delta['geometry'])

        #extract attributes
        attributes = RemoveNulls(delta['attributes'])

        #clean attributes
        for key in attributes.keys():
            
            #convert epoch timestamps to sql string
            if key.lower() in datetime_columns:
                timestamp = True
                try:
                    epoch = int(attributes[key])
                except:
                    timestamp = False
                if(timestamp):
                    if epoch < 0:
                        epoch = 0
                    attributes[key] = "DATEADD(S, {}, '1970-01-01')".format(epoch/1000)
                    
            else:
                #add quotes to strings, escape apostrophes
                if (not isinstance(attributes[key], float)) and (not isinstance(attributes[key], int)):
                    attributes[key] = str(attributes[key]).replace("'", "''")
                    attributes[key] = "'{}'".format(attributes[key])
                #convert everything else to a string for joining later
                else:
                    attributes[key] = str(attributes[key])

        #combine attributes and shape into one dict
        attributes.update({'shape': SHAPE})

        dict_out.append(attributes)

    return dict_out

##def WkbToSql(text):
##    SRID = '26910'
##    return 'STGeomFromText({})'.format(text)

def EditTable(query, connection, rowCount):
    cursor = connection.cursor()
    
    try:     
        cursor.execute(query)
    except:
        print('  Error executing SQL!\n')
        print('  Executed SQL query: {}\n'.format(query))
        print('  Rolling back SQL edits and exiting.')
        connection.rollback()
        return False

    return True

def Add(connection, fcName, dict_in):
    #add a feature to the versioned view of a featureclass
    keys = ','.join(dict_in.keys())
    values = ','.join(dict_in.values())

    try:
          globalId = dict_in['globalid']
    except:
          print('ERROR! Update object has no global ID!\n')
          print(json.dumps(dict_in))

    Debug('Adding object {}'.format(globalId), 2, indent=4)
    
    query = "INSERT INTO {}_evw ({}) VALUES ({});".format(fcName, keys, values) #TODO: make SRID variable
    
    return EditTable(query, connection, 1)

def Update(connection, fcName, dict_in):
    #update a feature in the versioned view of a featureclass

    try:
          globalId = dict_in['globalid']
    except:
          print('ERROR! Update object has no global ID!\n')
          print(json.dumps(dict_in))
          return False
          
    del dict_in['globalid']

    Debug('Updating object {}'.format(globalId), 2, indent=4)

    pairs = []
    
    for k,v in dict_in.items():       
        pairs.append('{}={}'.format(k, v))

    data = ','.join(pairs)

    query = "UPDATE {}_evw SET {} WHERE GLOBALID = {}".format(fcName, data, globalId) #TODO: make SRID variable

    return EditTable(query, connection, 1)
    

def Delete(connection, fcName, GUID):
    #remove feature from versioned view of featureclass

    Debug('Deleting object {}'.format(GUID), 2, indent=4)
    
    query = "DELETE FROM  {}_evw WHERE GLOBALID = {}".format(fcName, GUID)
    
    return EditTable(query, connection, 1)
    

def ExtractChanges(connection, fcName, lastGlobalIds, lastState, datatypes):
    #returns object lists for adds and updates, and list of objects deleted
    Debug('Extracting changes from {}...\n'.format(fcName), 1)
    
    #get global ids and changes from versioned view
    globalIds = GetGlobalIds(connection, fcName)
    changes = GetChanges(connection, fcName, lastState)

    #extrapolate updates and deletes
    Debug('Processing changes...', 2, indent=4)

    #missing ids = deletes
    deleteIds = list(set(lastGlobalIds).difference(globalIds))

    #get global ids from changes
    changeGlobalIds = set(changes['globalid'].tolist())

    #new ids = adds
    addIds = list(changeGlobalIds.difference(lastGlobalIds))

##    print('lastIds', lastGlobalIds)
##    print('ids', globalIds)
##    print('changed', changeGlobalIds)
##    print('deletes', deleteIds)
##    print('added', addIds)

    #get rows containing adds
    addRows = changes['globalid'].isin(addIds)

    #split changes into adds and updates
    adds = changes[addRows]
    updates = changes[~addRows]

    #print("ADDS:", adds, "\nUPDATES:",updates,"\nDELETES:",deleteGUIDs)

    adds_json = SqlToJson(adds, datatypes)
    updates_json = SqlToJson(updates, datatypes)


    deltas = {"adds": adds_json, "updates": updates_json, "deleteIds": deleteIds}

    Debug('Done.', 1, indent=4)
    
    return deltas

    #adds_out = []
    #parsed = json.loads(result)
    #print(json.dumps(parsed, indent=4))

def ApplyEdits(connection, fcName, json_dict, datatypes):
    #applies deltas to versioned view. Returns success codes and new SDE_STATE_ID
    Debug('Applying edits to {}...\n'.format(fcName), 1)
    
    adds_json = json_dict["adds"]
    updates_json = json_dict["updates"]
    
    deleteGUIDs = [delete.replace('{', '').replace('}', '') for delete in json_dict["deleteIds"]]

    adds = JsonToSql(adds_json, datatypes)
    updates = JsonToSql(updates_json, datatypes)

    for add in adds:
        if not Add(connection, fcName, add):
            return False

    for update in updates:
        if not Update(connection, fcName, update):
            return False

    for GUID in deleteGUIDs:
        if not Delete(connection, fcName, GUID):
            return False

    Debug('Done.\n', 1, indent=4)
    
    return True

#connection = Connect('inpredwgis2', 'REDWTest', 'REDW_Python', 'Benefit4u!123')
#GetCurrentStateId(connection)
#GetGlobalIds(connection, 'AGOL_TEST_PY_2')
#SqlToJson(GetChanges(connection, 'AGOL_TEST_PY_2', 0), GetDatatypes(connection, 'AGOL_TEST_PY_2'))
#print(GetSRID(connection, 'AGOL_TEST_PY_2'))
#datatypes = GetDatatypes(connection, 'AGOL_TEST_PY_2')
#print datatypes[datatypes['DATA_TYPE'].str.contains('datetime')]['COLUMN_NAME'].tolist()


