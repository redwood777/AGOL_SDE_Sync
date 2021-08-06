import pyodbc
import sys
import pandas as pd
import json
from shapely import wkt
import shapely
import geojson

def Connect(server, database, UID, PWD):
    connection_string = f'Driver={{SQL Server}};Server={server};Database={database};User Id={UID};Password={PWD}'
    #print(connection_string)
    
    try:
        connection = pyodbc.connect(connection_string)
        return connection
    except:
        print("Connection error:", sys.exc_info()[0])

def GetRegistrationId(connection, fcName):
    #Takes name of featureclass and returns registration id, or None if table has not been registered as versioned
    query = f"SELECT registration_id FROM SDE_table_registry WHERE table_name = '{fcName}'"
    data = pd.read_sql(query, connection)
    try:
        return data["registration_id"][0]
    except:
        print(f"'{fcName}' not found in SDE_table_registry. Check that it has been registered as versioned.")
        return None

def GetSdeStateIdsSinceId(connection, fcName, version, lastState):
    #Returns a list of SDE_STATE_IDs greater than lastState.
    cursor = connection.cursor()
    query = f"EXEC set_current_version '{version}'"
    cursor.execute(query)
    print(cursor.messages)
    query = f"SELECT SDE_STATE_ID FROM {fcName}_evw WHERE SDE_STATE_ID = {lastState}"
    data = pd.read_sql(query, connection)
    return data["SDE_STATE_ID"].tolist()

def GetDeletes(connection, registration_id, lastState):
    #returns list of objects deleted from versioned table registered with registration id since lastState
    query = f"SELECT SDE_DELETES_ROW_ID, DELETED_AT FROM D{registration_id} WHERE DELETED_AT > {lastState}"
    data = pd.read_sql(query, connection)
    return data #["SDE_DELETES_ROW_ID"].tolist()

def SdeObjectIdsToGlobalIds(connection, objectIds, fcName, registration_id):
    #returns UNORDERED list of global ids corresponding to objectIds, IN NO PARTICULAR ORDER
    objectIdsStr = ','.join(str(x) for x in objectIds)
    
    query = f"SELECT GLOBALID FROM {fcName} WHERE OBJECTID IN ({objectIdsStr})"
    data = pd.read_sql(query, connection)
    first_list = data["GLOBALID"].tolist()
    
    query = f"SELECT GLOBALID FROM a{registration_id} WHERE OBJECTID IN ({objectIdsStr})"
    data = pd.read_sql(query, connection)
    second_list = data["GLOBALID"].tolist()
    
    return first_list + list(set(second_list) - set(first_list))

    

def GetAdds(connection, registration_id, lastState):
    #returns list of objects in adds table since lastState

    #get rows from adds table since lastState
    query = f"SELECT * FROM a{registration_id} WHERE SDE_STATE_ID > {lastState}"
    adds = pd.read_sql(query, connection)

    #reaquire SHAPE column as Text
    query = f"SELECT SHAPE.STAsText() FROM a{registration_id} WHERE SDE_STATE_ID > {lastState}"
    shape = pd.read_sql(query, connection)
    #print(shape['SHAPE'])

    #replace shape column with text
    adds['SHAPE'] = shape.values

    return adds

def GeomTextToDict(text):
    #text = 'POLYGON ((400616.856061806 4640220.1292989273, 400528.97544409893 4640210.1569971107, 400502.5315446835 4640217.2087017745, 400507.11514948215 4640206.6311493963, 400598.9128298806 4640158.8985449821, 400616.856061806 4640220.1292989273))'
    geom = wkt.loads(text)
    #geomType = text.split('(')[0].strip().lower()

    #dict_out = {geom.geom_type: shapely.geometry.mapping(geom)['coordinates']}
    #print(json.dumps(dict_out))
    dict_out = geojson.Feature(geometry=geom, properties={})
    #dict_out = json.loads()
    print(json.dumps(dict_out, indent=4))

    #return dict_out
    #print(geom.geom_type)
    #dict_out = None
    
def DataframeToDict(df):
    #takes adds/updates dataframe and converts into agol-json-like dictionary
    dict_out = []

    #separate shape column from dataframe
    shapes = df['SHAPE']
    df = df.drop(columns='SHAPE')

    
    for i in df.index:
        #geometry = GeomTextToDict(shapes[i])
        geometry = {"wkt": shapes[i]}
        attributes = df.iloc[i-1]
        attributes = json.loads(attributes.to_json(orient='index'))
        #print(attributes)
        entry = {'geometry': geometry, 'attributes': attributes}
        dict_out.append(entry)

    

    return dict_out

    
def DeltasToJson(adds, updates, deleteGUIDs):
    adds_json = DataframeToDict(adds)
    updates_json = DataframeToDict(updates)

    deleteGUIDs = ['{{{0}}}'.format(delete) for delete in deleteGUIDs]

    dict_out = {"adds": adds_json, "updates": updates_json, "deletes": deleteGUIDs}
    print(json.dumps(dict_out, indent=4))

    return dict_out
        

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
    adds = adds.drop(columns=["SDE_STATE_ID", "OBJECTID"])

    #create new dataframes
    updates = adds.iloc[updateAddRows]
    adds = adds.drop(index=updateAddRows)
    deletes = deletes.drop(index=updateDeleteRows)
    
    print("converting delete ids to global")
    #get global ids for deletes
    deleteGUIDs = SdeObjectIdsToGlobalIds(connection, deletes["SDE_DELETES_ROW_ID"].tolist(), fcName, registration_id)

    #print("ADDS:", adds, "\nUPDATES:",updates,"\nDELETES:",deleteGUIDs)
    DeltasToJson(adds, updates, deleteGUIDs)
    
    

    #adds_out = []
    #parsed = json.loads(result)
    #print(json.dumps(parsed, indent=4))
        
        
        
    return None

def ApplyEdits(connection, fcName, registration_id, deltas):
    #applies deltas to versioned view. Returns success codes and new SDE_STATE_ID
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

    ExtractChanges(connection, registration_id, 'AGOL_TEST_PY_2', 0)
    #ids = GetSdeStateIdsSinceId(connection, 'AGOL_TEST_PY_2', 'DEFAULT', 0)
    
    #deletes = sql.GetDeletes(connection, registration_id, 0)
    #sql.SdeObjectIdsToGlobalIds(connection, deletes, 'AGOL_TEST_PY_2', registration_id)
    
    connection.close()

if __name__ == '__main__':
    test()


