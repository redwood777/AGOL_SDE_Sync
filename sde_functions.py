import pyodbc
import sys
import pandas as pd
import json
from arcpy import FromWKB, AsShape
#import shapely
#from shapely.geometry import shape
#import geojson

def Connect(server, database, UID, PWD):
    connection_string = 'Driver={{SQL Server}};Server={};Database={};User Id={};Password={}'.format(server, database, UID, PWD)
    #print(connection_string)
    
    try:
        connection = pyodbc.connect(connection_string)
        return connection
    except:
        print("Connection error:", sys.exc_info()[0])

def ReadSqlMessage(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    return (cursor.rowcount) #[0][1].split('[SQL Server]')[1])

def GetRegistrationId(connection, fcName):
    #Takes name of featureclass and returns registration id, or None if table has not been registered as versioned
    query = "SELECT registration_id FROM SDE_table_registry WHERE table_name = '{}'".format(fcName)
    data = pd.read_sql(query, connection)
    try:
        return data["registration_id"][0]
    except:
        print("'{}' not found in SDE_table_registry. Check that it has been registered as versioned.".format(fcName))
        return None

def GetCurrentStateId(connection):
    #returns current state id of DEFAULT version
    query = "SELECT state_id FROM SDE_versions WHERE NAME='DEFAULT'" #TODO: allow for other versions?
    response = pd.read_sql(query, connection)
    return (response['state_id'][0])

##def GetSyncs(connection, syncTableName):
##    #loads live syncs
##    query = "IF (EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{}')) PRINT 'true'; ELSE PRINT 'false';".format(syncTableName)
##    response = ReadSqlMessage(query, connection)
##    if (response == 'true'):
##        query = "SELECT * FROM {}".format(syncTableName)
##        reponse = pd.read_sql(query, connection)
##        print response
##    else:
##        query = "CREATE TABLE {} (NAME nvarchar, FIRST nvarchar, SECOND nvarchar, 

def GetStatesSince(connection, lastState):
    #Returns a list of SDE_STATE_IDs belonging to DEFAULT greater than lastState.
    query = "SELECT state_id FROM SDE_states WHERE lineage_name = 1 AND state_id > {}".format(lastState)
    data = pd.read_sql(query, connection)
    print data
    print lastState
    return ','.join([str(s) for s in data["state_id"].tolist()])

def RemoveNulls(dict_in):
    #returns dictionary with only non-null entries
    dict_in = {k: v for k, v in dict_in.items() if v is not None}

    return dict_in

def AddQuotes(dict_in):
    #adds quote marks to non-float values
    for k in dict_in.keys():
        if not isinstance(dict_in[k], float):
            dict_in[k] = "'{}'".format(dict_in[k])

    return dict_in

def SdeObjectIdsToGlobalIds(connection, objectIds, fcName, registration_id):
    #returns UNORDERED list of global ids corresponding to objectIds, IN NO PARTICULAR ORDER
    if len(objectIds) < 1:
        return []
    
    objectIdsStr = ','.join(str(x) for x in objectIds)
    
    query = "SELECT GLOBALID FROM {} WHERE OBJECTID IN ({})".format(fcName, objectIdsStr)
    data = pd.read_sql(query, connection)
    first_list = data["GLOBALID"].tolist()
    
    query = "SELECT GLOBALID FROM a{} WHERE OBJECTID IN ({})".format(registration_id, objectIdsStr)
    data = pd.read_sql(query, connection)
    second_list = data["GLOBALID"].tolist()
    
    return first_list + list(set(second_list) - set(first_list)) 

def GetAdds(connection, registration_id, states):
    #returns list of objects in adds table with state ids in states

    #get rows from adds table since lastState
    query = "SELECT * FROM a{} WHERE SDE_STATE_ID IN ({})".format(registration_id, states)
    adds = pd.read_sql(query, connection)

    #reaquire SHAPE column as WKB
    query = "SELECT SHAPE.STAsBinary() FROM a{} WHERE SDE_STATE_ID IN ({})".format(registration_id, states)
    shape = pd.read_sql(query, connection)
    #print(shape['SHAPE'])

    #replace shape column with text
    adds['SHAPE'] = shape.values
    print(adds['Visit_Year'])
    return adds

def GetDeletes(connection, registration_id, states):
    #returns list of objects deleted from versioned table registered with registration id since lastState
    query = "SELECT SDE_DELETES_ROW_ID, DELETED_AT FROM D{} WHERE DELETED_AT IN ({})".format(registration_id, states)
    data = pd.read_sql(query, connection)
    return data #["SDE_DELETES_ROW_ID"].tolist()

def WkbToEsri(WKB):
    #converts well known binary to esri json
    geom = FromWKB(WKB)
    return json.loads(geom.JSON)

def EsriToWkb(jsn):
    #converts esri json to well known binary
    jsn = json.dumps(jsn)
    geom = AsShape(jsn, True)
    #p = shapely.wkt.loads(wkt_text)
    #from shapely import wkb
    #return (wkb.dumps(p, hex=true))
    #print(geom.WKB)
    return geom.WKT

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

    
    for i in df.index:
        geometry = WkbToEsri(shapes[i])
        #geometry = {"wkt": shapes[i]}
        attributes = df.iloc[i-1]
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
 
def DeltasToJson(adds, updates, deleteGUIDs):
    #turns adds, updates and deletes in SQL form to JSON form
    adds_json = SqlToJson(adds)
    updates_json = SqlToJson(updates)

    deleteGUIDs = ['{{{0}}}'.format(delete) for delete in deleteGUIDs]

    dict_out = {"adds": adds_json, "updates": updates_json, "deleteIds": deleteGUIDs}
    print(json.dumps(dict_out, indent=4))

    return dict_out

def JsonToDeltas(json_dict):
    #turns JSON into adds, updates, and deletes in SQL form
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
    response = cursor.execute(query)
    
    checks = 0
    while(response.rowcount == -1):
        time.sleep(0.01)
        checks += 1

        if (checks > 100):
            print("SQL Query timed out!")
            return False

    print("Rows affected:", response.rowcount)
          
    if(response.rowcount != rowCount):
        print('Unexpected number of rows edited: {}\n'.format(response.rowcount))
        print('Executed SQL query: {}\n'.format(query))
        return False

    return True

def Add(connection, fcName, dict_in):
    #add a feature to the versioned view of a featureclass
    dict_in = RemoveNulls(dict_in)

    shape = dict_in['SHAPE']
    del dict_in['SHAPE']

    dict_in = AddQuotes(dict_in)

    keys = ','.join(dict_in.keys())
    values = ','.join(dict_in.values())
    
    query = "INSERT INTO {}_evw ({}, SHAPE) VALUES ({}, geometry::STGeomFromText('{}', 26910));".format(fcName, keys, values, shape) #TODO: make SRID variable
    print(query)
    EditTable(query, connection, 1)

def Update(connection, fcName, dict_in):
    #update a feature in the versioned view of a featureclass

    dict_in = RemoveNulls(dict_in)

    shape = dict_in['SHAPE']
    del dict_in['SHAPE']

    globalId = dict_in['GlobalID']
    del dict_in['GlobalID']

    dict_in = AddQuotes(dict_in)

    pairs = []
    
    for k,v in dict_in.items():       
        pairs.append('{}={}'.format(k, v))

    data = ','.join(pairs)
    print query
    query = "UPDATE {}_evw SET {}, SHAPE=geometry::STGeomFromText('{}',26910) WHERE GLOBALID = '{}';".format(fcName, data, shape, globalId) #TODO: make SRID variable

    EditTable(query, connection, 1)
    

def Delete(connection, fcName, GUID):
    #remove feature from versioned view of featureclass
    
    query = "DELETE FROM  {}_evw WHERE GLOBALID = '{}'".format(fcName, GUID)
    print(query)
    EditTable(query, connection, 1)
    

def ExtractChanges(connection, registration_id, fcName, lastState):
    #returns object lists for adds and updates, and list of objects deleted

    #get state ids for recent edits
    states = GetStatesSince(connection, lastState)
    
    #get adds and deletes from delta tables
    print("Getting adds")
    adds = GetAdds(connection, registration_id, states)
    print("Getting deletes")
    deletes = GetDeletes(connection, registration_id, states)

    #find updates from adds and deletes:
    
    print("Processing updates")
    
    #find updates, remove them from adds and deletes table, and add them to updates table
    #in SQL, updates are stored as an add and a delete occuring at the same SDE_STATE
    
    #create lists to store rows containing updates in adds and deletes table
    updateAddRows = []
    updateDeleteRows = []
    
    #find update rows
    for i in adds.index:
        for j in deletes.index:
            #check if both object id's and SDE_STATE_ID's match
            if (adds["OBJECTID"][i] == deletes["SDE_DELETES_ROW_ID"][j] and adds["SDE_STATE_ID"][i] == deletes["DELETED_AT"][j]):
                updateAddRows.append(i)
                updateDeleteRows.append(j)

    #drop state id and object id (these are specific to SDE and no longer needed)
    adds = adds.drop(labels=["SDE_STATE_ID", "OBJECTID"], axis='columns')

    #create new dataframes
    updates = adds.iloc[updateAddRows]
    adds = adds.drop(labels=updateAddRows)
    deletes = deletes.drop(labels=updateDeleteRows)
    
    print("converting delete ids to global")
    #get global ids for deletes
    deleteGUIDs = SdeObjectIdsToGlobalIds(connection, deletes["SDE_DELETES_ROW_ID"].tolist(), fcName, registration_id)

    #print("ADDS:", adds, "\nUPDATES:",updates,"\nDELETES:",deleteGUIDs)
    jsn = DeltasToJson(adds, updates, deleteGUIDs)
    
    return jsn

    #adds_out = []
    #parsed = json.loads(result)
    #print(json.dumps(parsed, indent=4))

def ApplyEdits(connection, registration_id, fcName, json_dict):
    #applies deltas to versioned view. Returns success codes and new SDE_STATE_ID
    print('Applying eidts')
    adds, updates, deleteGUIDs = JsonToDeltas(json_dict)

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
    #data = pd.read_sql(query, connection)
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


