import pyodbc
import sys
import pandas as pd
import json
from arcpy import FromWKB, AsShape
from ui_functions import Debug
import time
#import shapely
#from shapely.geometry import shape
#import geojson

def Connect(server, database, UID, PWD):
    Debug('Connecting to SQL Server\n', 2)
    
    connection_string = 'Driver={{SQL Server}};Server={};Database={};User Id={};Password={}'.format(server, database, UID, PWD)

    Debug('   SQL Connection string: "{}"\n'.format(connection_string), 3)
    
    try:
        connection = pyodbc.connect(connection_string)
        return connection
    except:
        print("   Connection error:", sys.exc_info()[0])

def ReadSqlMessage(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    return (cursor.rowcount) #[0][1].split('[SQL Server]')[1])

def ReadSQLWithDebug(query, connection):
    Debug('SQL Query: "{}"\n'.format(query), 3)
    return pd.read_sql(query, connection)
          

def GetRegistrationId(connection, fcName):
    #Takes name of featureclass and returns registration id, or None if table has not been registered as versioned
    
    Debug('Getting registration id for "{}"...'.format(fcName), 2)
    
    query = "SELECT registration_id FROM SDE_table_registry WHERE table_name = '{}'".format(fcName)
    
    data = ReadSQLWithDebug(query, connection)
    
    try:
        registration_id = data["registration_id"][0]
    except:
        print("   '{}' not found in SDE_table_registry. Check that it has been registered as versioned.\n".format(fcName))
        return None

    Debug('   Registration id: {}\n'.format(registration_id), 2)
    return registration_id

def GetCurrentStateId(connection):
    #returns current state id of DEFAULT version
    Debug('Getting current SDE state id...', 2)
    
    query = "SELECT state_id FROM SDE_versions WHERE NAME='DEFAULT'" #TODO: allow for other versions?
    response = ReadSQLWithDebug(query, connection)

    try:
        state_id = response['state_id'][0]
    except:
        print('   Fatal error! Could not aquire current state id.\n')
        exit()

    Debug('   SDE state id: {}\n'.format(state_id), 2)
    return int(state_id)

##def GetSyncs(connection, syncTableName):
##    #loads live syncs
##    query = "IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{}')) PRINT 'true'; ELSE PRINT 'false';".format(syncTableName)
##    response = ReadSqlMessage(query, connection)
##    if (response == 'true'):
##        query = "SELECT * FROM {}".format(syncTableName)
##        reponse = ReadSQLWithDebug(query, connection)
##        print response
##    else:
##        query = "CREATE TABLE {} (NAME nvarchar, FIRST nvarchar, SECOND nvarchar, 

def GetStatesSince(connection, lastState):
    #Returns a list of SDE_STATE_IDs belonging to DEFAULT greater than lastState, and less than current state (in progress edits will have state ids > current state).

    Debug('Getting DEFAULT version SDE states since state {}\n'.format(lastState), 2)

    currentState = GetCurrentStateId(connection)
        
    query = "SELECT state_id FROM SDE_states WHERE lineage_name = 1 AND state_id > {} AND state_id <= {}".format(lastState, currentState)
    data = ReadSQLWithDebug(query, connection)
    
    return ','.join([str(s) for s in data["state_id"].tolist()])

def RemoveNulls(dict_in):
    #returns dictionary with only non-null entries
    dict_in = {k: v for k, v in dict_in.items() if v is not None}

    return dict_in

def AddQuotes(dict_in):
    #adds quote marks to non-float values, turns all values into strings, escapes apostrophes
    for k in dict_in.keys():           
        if not isinstance(dict_in[k], float):
            dict_in[k] = str(dict_in[k]).replace("'", "''")
            dict_in[k] = "'{}'".format(dict_in[k])
        else:
            dict_in[k] = str(dict_in[k])
        

    return dict_in

def SdeObjectIdsToGlobalIds(connection, objectIds, fcName, registration_id):
    #returns UNORDERED list of global ids corresponding to objectIds, IN NO PARTICULAR ORDER

    Debug('Converting object IDs to global IDs\n', 2)
    
    
    if len(objectIds) < 1:
        return []
    
    objectIdsStr = ','.join(str(x) for x in objectIds)

    Debug('   Object ids: {}\n'.format(objectIdsStr), 3)
    
    query = "SELECT GLOBALID FROM {} WHERE OBJECTID IN ({})".format(fcName, objectIdsStr)
    data = ReadSQLWithDebug(query, connection)
    first_list = data["GLOBALID"].tolist()
    
    query = "SELECT GLOBALID FROM a{} WHERE OBJECTID IN ({})".format(registration_id, objectIdsStr)
    data = ReadSQLWithDebug(query, connection)
    second_list = data["GLOBALID"].tolist()
    
    return first_list + list(set(second_list) - set(first_list))

def GetGlobalIds(connection, fcName):
    #returns list of global ids existing in featureclass
    Debug('Getting SDE Global IDs...', 2)

    query = "SELECT GLOBALID FROM {}_evw".format(fcName)
    globalIds = ReadSQLWithDebug(query, connection)

    return globalIds['GLOBALID'].tolist()

def GetChanges(connection, fcName, stateId):
    #returns rows from versioned view with state id > state

    Debug('Getting SDE changes...', 1)

    #get rows from adds table since lastState
    query = "SELECT * FROM {}_evw WHERE SDE_STATE_ID > {}".format(fcName, stateId)
    adds = ReadSQLWithDebug(query, connection)

    if(len(adds.index) > 0):
        #reaquire SHAPE column as WKB
        query = "SELECT SHAPE.STAsBinary() FROM {}_evw WHERE SDE_STATE_ID > {}".format(fcName, stateId)
        shape = ReadSQLWithDebug(query, connection)
        #print(shape['SHAPE'])

        #replace shape column with text
        adds['SHAPE'] = shape.values

    return adds

##def GetAdds(connection, registration_id, states):
##    #returns list of objects in adds table with state ids in states
##
##    Debug('Getting adds...', 1)
##
##    #get rows from adds table since lastState
##    query = "SELECT * FROM a{} WHERE SDE_STATE_ID IN ({})".format(registration_id, states)
##    adds = ReadSQLWithDebug(query, connection)
##
##    if(len(adds.index) > 0):
##        #reaquire SHAPE column as WKB
##        query = "SELECT SHAPE.STAsBinary() FROM a{} WHERE SDE_STATE_ID IN ({})".format(registration_id, states)
##        shape = ReadSQLWithDebug(query, connection)
##        #print(shape['SHAPE'])
##
##        #replace shape column with text
##        adds['SHAPE'] = shape.values
##
##    return adds

##def GetDeletes(connection, registration_id, states):
##    #returns list of objects deleted from versioned table registered with registration id since lastState
##
##    Debug('Getting deletes...', 1)
##    
##    query = "SELECT SDE_DELETES_ROW_ID, DELETED_AT FROM D{} WHERE DELETED_AT IN ({})".format(registration_id, states)
##    data = ReadSQLWithDebug(query, connection)
##    return data #["SDE_DELETES_ROW_ID"].tolist()

def WkbToEsri(WKB):
    #converts well known binary to esri json
    Debug('   Converting WKB to Esri Json', 2)
    Debug('      WKB (as hex): {}'.format(WKB), 3)
    
    geom = FromWKB(WKB)
    esri = geom.JSON
    
    Debug('      Converted Esri Json: {}'.format(esri), 3)
    return json.loads(esri)

def EsriToWkb(jsn):
    #converts esri json to well known text

    Debug('   Converting Esri Json to WKT', 2)
    
    jsn = json.dumps(jsn)
    
    Debug('      Esri Json: {}\n'.format(jsn), 3)
          
    geom = AsShape(jsn, True)
    #p = shapely.wkt.loads(wkt_text)
    #from shapely import wkb
    #return (wkb.dumps(p, hex=true))
    #print(geom.WKB)
    wkt = geom.WKT

    Debug('      Converted WKT: {}\n'.format(wkt), 3)
    
    return wkt

##def WktToGeoJson(text):
##    #text = 'POLYGON ((400616.856061806 4640220.1292989273, 400528.97544409893 4640210.1569971107, 400502.5315446835 4640217.2087017745, 400507.11514948215 4640206.6311493963, 400598.9128298806 4640158.8985449821, 400616.856061806 4640220.1292989273))'
##    geom = wkt.loads(text)
##    #geomType = text.split('(')[0].strip().lower()
##
##    #dict_out = {geom.geom_type: shapely.geometry.mapping(geom)['coordinates']}
##    #print(json.dumps(dict_out))
##    dict_out = geojson.Feature(geometry=geom, properties={})
##    #dict_out = json.loads()
##    #print(json.dumps(dict_out, indent=4))
##
##    return dict_out['geometry']
##    #print(geom.geom_type)
##    #dict_out = None
##
##def GeoJsonToWkt(dict_in):
##    geom = shape(dict_in)
##
##    # Now it's very easy to get a WKT/WKB representation
##    return geom.wkt
    
def SqlToJson(df):
    #takes adds or updates dataframe and converts into agol-json-like dictionary
    dict_out = []

    #separate shape column from dataframe
    shapes = df['SHAPE']
    df = df.drop(labels='SHAPE', axis='columns')
    
    for i in range(0, len(df.index)):
        geometry = WkbToEsri(shapes.iloc[i])
        #geometry = {"wkt": shapes[i]}
        attributes = df.iloc[i]
        attributes = json.loads(attributes.to_json(orient='index'))
        attributes = RemoveNulls(attributes)
        #print(attributes)
        entry = {'geometry': geometry, 'attributes': attributes}
        dict_out.append(entry)

    return dict_out

def JsonToSql(deltas):
    #takes adds or updates json and turns it into sql-writable format
    dict_out = []
    
    for delta in deltas:
        #turn geometry json into syntax for SQL
        SHAPE = EsriToWkb(delta['geometry'])

        #extract attributes
        attributes = delta['attributes']

        #combine attributes and shape into one dict
        attributes.update({'SHAPE': SHAPE})

        dict_out.append(attributes)

    return dict_out
 
def SqlDeltasToJson(adds, updates, deleteGUIDs):
    #turns adds, updates and deletes in SQL form to JSON form

    Debug('Converting SQL adds, updates, and deletes to json...\n', 2)
    adds_json = SqlToJson(adds)
    updates_json = SqlToJson(updates)

    #deleteGUIDs = ['{{{0}}}'.format(delete) for delete in deleteGUIDs]

    dict_out = {"adds": adds_json, "updates": updates_json, "deleteIds": deleteGUIDs}

    return dict_out

def JsonToSqlDeltas(json_dict):
    #turns JSON into adds, updates, and deletes in SQL form

    Debug('Converting json to adds, updates, and deletes for SQL...\n', 2)
        
    adds_json = json_dict["adds"]
    updates_json = json_dict["updates"]
    
    deleteGUIDs = [delete.replace('{', '').replace('}', '') for delete in json_dict["deleteIds"]]

    adds = JsonToSql(adds_json)
    updates = JsonToSql(updates_json)

    return adds, updates, deleteGUIDs

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
        connection.close()
        exit()
        return False

    return True
    
##    checks = 0
##    while(cursor.rowcount == -1):
##        time.sleep(0.05)
##        checks += 1
##
##        if (checks > 200):
##            print("  SQL Query timed out!\n")
##            
##            return False

##    Debug("  Rows affected: {}\n".format(cursor.rowcount), 2)
##          
##    if(cursor.rowcount != rowCount):
##        print('  Unexpected number of rows affected: {}\n'.format(cursor.rowcount))
##        print('  Executed SQL query: {}\n'.format(query))
##        return False

def Add(connection, fcName, dict_in):
    #add a feature to the versioned view of a featureclass
    dict_in = RemoveNulls(dict_in)

    shape = dict_in['SHAPE']
    del dict_in['SHAPE']

    dict_in = AddQuotes(dict_in)

    keys = ','.join(dict_in.keys())
    values = ','.join(dict_in.values())

    try:
          globalId = dict_in['GlobalID']
    except:
          print('ERROR! Update object has no global ID!\n')
          print(json.dumps(dict_in))

    Debug('Adding object {}\n'.format(globalId), 2)
    
    query = "INSERT INTO {}_evw ({}, SHAPE) VALUES ({}, geometry::STGeomFromText('{}', 26910));".format(fcName, keys, values, shape) #TODO: make SRID variable
    
    return EditTable(query, connection, 1)

def Update(connection, fcName, dict_in):
    #update a feature in the versioned view of a featureclass

    dict_in = RemoveNulls(dict_in)

    shape = dict_in['SHAPE']
    del dict_in['SHAPE']

    globalId = dict_in['GlobalID']
    del dict_in['GlobalID']

    Debug('Updating object {}\n'.format(globalId), 2)

    dict_in = AddQuotes(dict_in)

    pairs = []
    
    for k,v in dict_in.items():       
        pairs.append('{}={}'.format(k, v))

    data = ','.join(pairs)

    query = "UPDATE {}_evw SET {}, SHAPE=geometry::STGeomFromText('{}',26910) WHERE GLOBALID = '{}';".format(fcName, data, shape, globalId) #TODO: make SRID variable

    return EditTable(query, connection, 1)
    

def Delete(connection, fcName, GUID):
    #remove feature from versioned view of featureclass

    Debug('Deleting object {}\n'.format(GUID), 2)
    
    query = "DELETE FROM  {}_evw WHERE GLOBALID = '{}'".format(fcName, GUID)
    
    return EditTable(query, connection, 1)
    

def ExtractChanges(connection, registration_id, fcName, lastGlobalIds, lastState):
    #returns object lists for adds and updates, and list of objects deleted
    print('Extracting changes from {}...\n'.format(fcName))

    #get state ids for recent edits
    states = GetStatesSince(connection, lastState)

    if(len(states) < 1):
        #No state ids to check
        return {'adds': [], 'updates':[], 'deleteIds': []}
    
    #get adds and deletes from delta tables
##  adds = GetAdds(connection, registration_id, states)
##  deletes = GetDeletes(connection, registration_id, states)
    
    #get global ids and changes from versioned view
    globalIds = GetGlobalIds(connection, fcName)
    changes = GetChanges(connection, fcName, lastState)

    #extrapolate updates and deletes
    Debug('Processing changes...', 1)

    #missing ids = deletes
    deleteIds = list(set(lastGlobalIds).difference(globalIds))

    #get global ids from changes
    changeGlobalIds = set(changes['GlobalID'].tolist())

    #new ids = adds
    addIds = list(changeGlobalIds.difference(lastGlobalIds))

    print('lastIds', lastGlobalIds)
    print('ids', globalIds)
    print('changed', changeGlobalIds)
    print('deletes', deleteIds)
    print('added', addIds)

    #get rows containing adds
    addRows = changes['GlobalID'].isin(addIds)

    #split changes into adds and updates
    adds = changes[addRows]
    updates = changes[~addRows]
    
    
    #find updates, remove them from adds and deletes table, and add them to updates table
    #in SQL, updates are stored as an add and a delete occuring at the same SDE_STATE
    
    #create lists to store rows containing updates in adds and deletes table
##    updateAddRows = []
##    updateDeleteRows = []
##    
##    #find update rows
##    for i in adds.index:
##        for j in deletes.index:
##            #check if both object id's and SDE_STATE_ID's match
##            if (adds["OBJECTID"][i] == deletes["SDE_DELETES_ROW_ID"][j] and adds["SDE_STATE_ID"][i] == deletes["DELETED_AT"][j]):
##                updateAddRows.append(i)
##                updateDeleteRows.append(j)
##
##    #drop state id and object id (these are specific to SDE and no longer needed)
##    adds = adds.drop(labels=["SDE_STATE_ID", "OBJECTID"], axis='columns')

    #create new dataframes
##    updates = adds.iloc[updateAddRows]
##    adds = adds.drop(labels=updateAddRows)
##    deletes = deletes.drop(labels=updateDeleteRows)
    
    #get global ids for deletes
    #deleteGUIDs = SdeObjectIdsToGlobalIds(connection, deletes["SDE_DELETES_ROW_ID"].tolist(), fcName, registration_id)

    #print("ADDS:", adds, "\nUPDATES:",updates,"\nDELETES:",deleteGUIDs)
    deltas = SqlDeltasToJson(adds, updates, deleteIds)
    
    return deltas

    #adds_out = []
    #parsed = json.loads(result)
    #print(json.dumps(parsed, indent=4))

def ApplyEdits(connection, registration_id, fcName, json_dict):
    #applies deltas to versioned view. Returns success codes and new SDE_STATE_ID
    print('Applying edits...\n')
    adds, updates, deleteGUIDs = JsonToSqlDeltas(json_dict)

    for add in adds:
        Add(connection, fcName, add)

    for update in updates:
        Update(connection, fcName, update)

    for GUID in deleteGUIDs:
        Delete(connection, fcName, GUID)
    
    return None

#GeomTextToDict('')

def test():
    connection = Connect('inpredwgis2', 'REDWTest', 'REDW_Python', 'Benefit4u!123')
    
    #cursor = connection.cursor()
    #query = "UPDATE AGOL_TEST_PY_2_evw SET Taxonomy = 'Obla dee' WHERE OBJECTID = 484
    # execute the query and read to a dataframe in Python
    #data = ReadSQLWithDebug(query, connection)
    #print(data)


    GetSyncs(connection, 'AGOL_TEST_PY_2')

    #registration_id = GetRegistrationId(connection, 'AGOL_TEST_PY_2')

    #json_dict = ExtractChanges(connection, registration_id, 'AGOL_TEST_PY_2', 0)

    #ApplyEdits(connection, registration_id, 'AGOL_TEST_PY_2', json_dict)
    #ids = GetSdeStateIdsSinceId(connection, 'AGOL_TEST_PY_2', 'DEFAULT', 0)
    
    #deletes = sql.GetDeletes(connection, registration_id, 0)
    #sql.SdeObjectIdsToGlobalIds(connection, deletes, 'AGOL_TEST_PY_2', registration_id)
    
    connection.close()

if __name__ == '__main__':
    import main
    main.test()


