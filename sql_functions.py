import pyodbc
import sys
import pandas as pd
import json
#import arcpy
from shapely import wkt
from shapely.geometry import shape
import geojson

def Connect(server, database, UID, PWD):
    connection_string = 'Driver={{SQL Server}};Server={};Database={};User Id={};Password={}'.format(server, database, UID, PWD)
    #print(connection_string)
    
    try:
        connection = pyodbc.connect(connection_string)
        return connection
    except:
        print("Connection error:", sys.exc_info()[0])

def GetRegistrationId(connection, fcName):
    #Takes name of featureclass and returns registration id, or None if table has not been registered as versioned
    query = "SELECT registration_id FROM SDE_table_registry WHERE table_name = '{}'".format(fcName)
    data = pd.read_sql(query, connection)
    try:
        return data["registration_id"][0]
    except:
        print("'{}' not found in SDE_table_registry. Check that it has been registered as versioned.".format(fcName))
        return None

def GetSdeStateIdsSinceId(connection, fcName, version, lastState):
    #Returns a list of SDE_STATE_IDs greater than lastState.
    cursor = connection.cursor()
    query = "EXEC set_current_version '{}'".format(version)
    cursor.execute(query)
    print(cursor.messages)
    query = "SELECT SDE_STATE_ID FROM {}_evw WHERE SDE_STATE_ID = {}".format(fcName, lastState)
    data = pd.read_sql(query, connection)
    return data["SDE_STATE_ID"].tolist()

def GetDeletes(connection, registration_id, lastState):
    #returns list of objects deleted from versioned table registered with registration id since lastState
    query = "SELECT SDE_DELETES_ROW_ID, DELETED_AT FROM D{} WHERE DELETED_AT > {}".format(registration_id, lastState)
    data = pd.read_sql(query, connection)
    return data #["SDE_DELETES_ROW_ID"].tolist()

def SdeObjectIdsToGlobalIds(connection, objectIds, fcName, registration_id):
    #returns UNORDERED list of global ids corresponding to objectIds, IN NO PARTICULAR ORDER
    objectIdsStr = ','.join(str(x) for x in objectIds)
    
    query = "SELECT GLOBALID FROM {} WHERE OBJECTID IN ({})".format(fcName, objectIdsStr)
    data = pd.read_sql(query, connection)
    first_list = data["GLOBALID"].tolist()
    
    query = "SELECT GLOBALID FROM a{} WHERE OBJECTID IN ({})".format(registration_id, objectIdsStr)
    data = pd.read_sql(query, connection)
    second_list = data["GLOBALID"].tolist()
    
    return first_list + list(set(second_list) - set(first_list))

    

def GetAdds(connection, registration_id, lastState):
    #returns list of objects in adds table since lastState

    #get rows from adds table since lastState
    query = "SELECT * FROM a{} WHERE SDE_STATE_ID > {}".format(registration_id, lastState)
    adds = pd.read_sql(query, connection)

    #reaquire SHAPE column as Text
    query = "SELECT SHAPE.STAsText() FROM a{} WHERE SDE_STATE_ID > {}".format(registration_id, lastState)
    shape = pd.read_sql(query, connection)
    #print(shape['SHAPE'])

    #replace shape column with text
    adds['SHAPE'] = shape.values

    return adds

def WktToGeoJson(text):
    #text = 'POLYGON ((400616.856061806 4640220.1292989273, 400528.97544409893 4640210.1569971107, 400502.5315446835 4640217.2087017745, 400507.11514948215 4640206.6311493963, 400598.9128298806 4640158.8985449821, 400616.856061806 4640220.1292989273))'
    geom = wkt.loads(text)
    #geomType = text.split('(')[0].strip().lower()

    #dict_out = {geom.geom_type: shapely.geometry.mapping(geom)['coordinates']}
    #print(json.dumps(dict_out))
    dict_out = geojson.Feature(geometry=geom, properties={})
    #dict_out = json.loads()
    #print(json.dumps(dict_out, indent=4))

    return dict_out['geometry']
    #print(geom.geom_type)
    #dict_out = None

def GeoJsonToWkt(dict_in):
    geom = shape(dict_in)

    # Now it's very easy to get a WKT/WKB representation
    return geom.wkt
    
def DataframeToDict(df):
    #takes adds/updates dataframe and converts into agol-json-like dictionary
    dict_out = []

    #separate shape column from dataframe
    shapes = df['SHAPE']
    df = df.drop(labels='SHAPE', axis='columns')

    
    for i in df.index:
        geometry = WktToGeoJson(shapes[i])
        #geometry = {"wkt": shapes[i]}
        attributes = df.iloc[i-1]
        attributes = json.loads(attributes.to_json(orient='index'))
        #print(attributes)
        entry = {'geometry': geometry, 'attributes': attributes}
        dict_out.append(entry)

    return dict_out

def JsonToSql(deltas):
    dict_out = []
    
    for delta in deltas:
        SHAPE = WktToSql(GeoJsonToWkt(delta['geometry']))
        attributes = delta['attributes']

        attributes.update({'SHAPE': SHAPE})

        dict_out.append(attributes)

    return dict_out
 
def DeltasToJson(adds, updates, deleteGUIDs):
    #turns adds, updates and deletes in SQL form to JSON form
    adds_json = DataframeToDict(adds)
    updates_json = DataframeToDict(updates)

    deleteGUIDs = ['{{{0}}}'.format(delete) for delete in deleteGUIDs]

    dict_out = {"adds": adds_json, "updates": updates_json, "deletes": deleteGUIDs}
    #print(json.dumps(dict_out, indent=4))

    return dict_out

def JsonToDeltas(json_dict):
    #turns JSON into adds, updates, and deletes in SQL form
    adds_json = json_dict["adds"]
    updates_json = json_dict["updates"]
    
    deleteGUIDs = [delete.replace('{', '').replace('}', '') for delete in json_dict["deletes"]]

    adds = JsonToSql(adds_json)
    updates = JsonToSql(updates_json)

    return adds, updates, deleteGUIDs

def WktToSql(text):
    SRID = '26910'
    return 'STGeomFromText({}, {})'.format(text, SRID)

def DictToKeyValues(dict_in):
    dict_in = {k: v for k, v in dict_in.items() if v is not None}
    keys = ','.join(dict_in.keys())
    values = ','.join(dict_in.values())

    return keys, values

def Add(connection, fcName, dict_in):
    keys, values = DictToKeyValues(dict_in)
    
    query = 'INSERT INTO table_name {}_evw ({}) VALUES ({});'.format(fcName, keys, values)
    print(query)
    #result = pd.read_sql(connection, query)

#def Update(
    

def ExtractChanges(connection, registration_id, fcName, lastState):
    #returns object lists for adds and updates, and list of objects deleted

    #get adds and deletes from delta tables
    print("getting adds")
    adds = GetAdds(connection, registration_id, lastState)
    print("getting deletes")
    deletes = GetDeletes(connection, registration_id, lastState)

    #find updates from adds and deletes:
    #print(adds.index)
    #updates = adds.loc(adds["OBJECTID"] in (deletes["SDE_DELETES_ROW_ID"].tolist()))
    #print(updates)

    #find updates, remove them from adds and deletes table, and add them to updates table
    #in SQL, updates are stored as an add and a delete occuring at the same SDE_STATE
    print("processing updates")
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
    json = DeltasToJson(adds, updates, deleteGUIDs)
    
    return json

    #adds_out = []
    #parsed = json.loads(result)
    #print(json.dumps(parsed, indent=4))
        
    return None

def ApplyEdits(connection, registration_id, fcName, json_dict):
    #applies deltas to versioned view. Returns success codes and new SDE_STATE_ID

    adds, updates, deleteGUIDs = JsonToDeltas(json_dict)

    for add in adds:
        Add(connection, fcName, add)
    
    return None

#GeomTextToDict('')

def test():
    connection = Connect('inpredwgis2', 'REDWTest', 'REDW_Python', 'Benefit4u!123')
    
    #cursor = connection.cursor()
    #query = "UPDATE AGOL_TEST_PY_2_evw SET Taxonomy = 'Obla dee' WHERE OBJECTID = 484
    # execute the query and read to a dataframe in Python
    #data = pd.read_sql(query, connection)
    #print(data)

    registration_id = GetRegistrationId(connection, 'AGOL_TEST_PY_2')

    json_dict = ExtractChanges(connection, registration_id, 'AGOL_TEST_PY_2', 0)

    ApplyEdits(connection, registration_id, 'AGOL_TEST_PY_2', json_dict)
    #ids = GetSdeStateIdsSinceId(connection, 'AGOL_TEST_PY_2', 'DEFAULT', 0)
    
    #deletes = sql.GetDeletes(connection, registration_id, 0)
    #sql.SdeObjectIdsToGlobalIds(connection, deletes, 'AGOL_TEST_PY_2', registration_id)
    
    connection.close()

if __name__ == '__main__':
    test()


