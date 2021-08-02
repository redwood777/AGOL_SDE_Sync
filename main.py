import json
import sql_functions as sql
#import json_functions as jsn

def ResolveConflicts(SDE_deltas, AGOL_deltas):
    #Finds all conflicting edits. Resolves conflicts by user input. Returns revised SDE_deltas and AGOL_deltas
    return SDE_deltas, AGOL_deltas

def main():
    #get database, FC name
    server = 'inpredwgis2'
    database = 'REDWTest'
    fc = 'AGOL_TEST_PY_2'
    
    #check if FC has been set up before (cached in SQL table?)
        #if so, get last serverGen/SDE_STATE_ID
        #if not, ask for service URL and cache
    
    #connect to database
    UID = 'REDW_Python'
    PWD = 'Benefit4u!123'
    
    cnxn = sql.Connect(server, database, UID, PWD)
    #check if FC has been registered as versioned
    #check that service has been set up correctly
    #extract changes from SQL
    #extract changes from AGOL
    #reconcile
    #apply & commit changes to SQL
    #apply changes to AGOL
    #cache new serverGen/SDE_STATE_ID
    
    

def test():
    connection = sql.Connect('inpredwgis2', 'REDWTest', 'REDW_Python', 'Benefit4u!123')
    
    #cursor = connection.cursor()
    #query = "UPDATE AGOL_TEST_PY_2_evw SET Taxonomy = 'Obla dee' WHERE OBJECTID = 484
    # execute the query and read to a dataframe in Python
    #data = pd.read_sql(query, connection)
    #print(data)

    registration_id = sql.GetRegistrationId(connection, 'AGOL_TEST_PY_2')

    sql.ExtractChanges(connection, registration_id, 'AGOL_TEST_PY_2', 0)
    #ids = GetSdeStateIdsSinceId(connection, 'AGOL_TEST_PY_2', 'DEFAULT', 0)
    
    #deletes = sql.GetDeletes(connection, registration_id, 0)
    #sql.SdeObjectIdsToGlobalIds(connection, deletes, 'AGOL_TEST_PY_2', registration_id)
    
    connection.close()

if __name__ == '__main__':
    test()
