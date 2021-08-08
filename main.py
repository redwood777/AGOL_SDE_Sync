import json
import copy
#import sql_functions as sql
#import json_functions as jsn

def GetGlobalIds(dict_in):
    #pulls global ids from adds or updates dictionary, returns as set
    return {add['attributes']['GlobalID'] for add in dict_in}

def Options(prompt, menu):
    i = 0
    print(prompt)
    for item in menu:
        print(i, "--", item)
        i+=1
    return input()

def ResolveConflicts(SDE_deltas, AGOL_deltas):
    #Finds all conflicting edits. Resolves conflicts by user input. Returns revised SDE_deltas and AGOL_deltas
    #SDE_added = GetGlobalIds(SDE_deltas['adds'])
    SDE_updated = GetGlobalIds(SDE_deltas['updates'])
    AGOL_updated = GetGlobalIds(AGOL_deltas['updates'])
    
    #remove deletes that have already occured in destination
    print(AGOL_deltas['deleteIds'])
    print(SDE_deltas['deleteIds'])
    SDE_deleted = set(SDE_deltas['deleteIds']).difference(AGOL_deltas['deleteIds'])
    AGOL_deleted = set(AGOL_deltas['deleteIds']).difference(SDE_deltas['deleteIds'])

    print("SDE_deleted:", SDE_deleted)
    print("AGOL_deleted:", AGOL_deleted)

    #find update/delete conflictions
    AGOL_updated_SDE_deleted = AGOL_updated.intersection(SDE_deleted)
    SDE_updated_AGOL_deleted = SDE_updated.intersection(AGOL_deleted)

    #find update/update conflictions
    both_updated = AGOL_updated.intersection(SDE_updated)

    print("AGOL_updated_SDE_deleted:", AGOL_updated_SDE_deleted)
    print("SDE_updated_AGOL_deleted:", SDE_updated_AGOL_deleted)
    print("both_updated:", both_updated)

    for ID in AGOL_updated_SDE_deleted:
        print()
    
       
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
    deltas = {  
        "adds": [  
          {  
            "geometry": {  
              "rings": [  
                [  
                  [  
                    1599093.38156825,
                    4299494.38162189
                  ],
                  [  
                    1621892.61012839,
                    4282639.631925
                  ],
                  [  
                    1616369.15773174,
                    4273287.47109171
                  ],
                  [  
                    1596005.6876463,
                    4284510.52152801
                  ],
                  [  
                    1599093.38156825,
                    4299494.38162189
                  ]
                ]
              ]
            },
            "attributes": {  
              "FID": 250,
              "GlobalID": "C8FCEBF0-51D1-4FFA-A5ED-FFD47F10014F",
              "ObjectID": 125,
              "FIPS_CNTRY": "MT",
              "GMI_CNTRY": "MLT",
              "ISO_2DIGIT": "MT",
              "ISO_3DIGIT": "MLT",
              "ISO_NUM": 470,
              "CNTRY_NAME": "Malta",
              "LONG_NAME": "Republic of Malta",
              "ISOSHRTNAM": "Malta",
              "UNSHRTNAM": "Malta",
              "LOCSHRTNAM": "Malta",
              "LOCLNGNAM": "Repubblika ta' Malta",
              "STATUS": "UN Member State",
              "POP2007": 401880,
              "SQKM": 211.5,
              "SQMI": 81.66,
              "LAND_SQKM": 316,
              "COLORMAP": 2
            }
          }
        ],
        "updates": [  
          {  
            "geometry": {  
              "rings": [  
                [  
                  [  
                    1599093.38156825,
                    4299494.38162189
                  ],
                  [  
                    1621892.61012839,
                    4282639.631925
                  ],
                  [  
                    1616369.15773174,
                    4273287.47109171
                  ],
                  [  
                    1596005.6876463,
                    4284510.52152801
                  ],
                  [  
                    1599093.38156825,
                    4299494.38162189
                  ]
                ]
              ]
            },
            "attributes": {  
              "FID": 1,
              "GlobalID": "CECC5D06-CFD4-40E7-943B-3793770411E1",
              "ObjectID": 125,
              "FIPS_CNTRY": "MT",
              "GMI_CNTRY": "MLT",
              "ISO_2DIGIT": "MT",
              "ISO_3DIGIT": "MLT",
              "ISO_NUM": 470,
              "CNTRY_NAME": "Malta",
              "LONG_NAME": "Republic of Malta",
              "ISOSHRTNAM": "Malta",
              "UNSHRTNAM": "Malta",
              "LOCSHRTNAM": "Malta",
              "LOCLNGNAM": "Repubblika ta' Malta",
              "STATUS":" UN Member State",
              "POP2007": 401880,
              "SQKM": 211.5,
              "SQMI": 81.66,
              "LAND_SQKM": 316,
              "COLORMAP": 2
            }
          }
        ],
        "deleteIds": [  
          "0D8E1D93-29AE-4D16-AF61-E74FED983732"
        ]
      }



    deltas2 = copy.deepcopy(deltas)
    deltas2['deleteIds'].append("CECC5D06-CFD4-40E7-943B-3793770411E1")

    #ResolveConflicts(deltas, deltas2)
    print(Options('select', ['a', 'b', 'c']))
 
    #connection = sql.Connect('inpredwgis2', 'REDWTest', 'REDW_Python', 'Benefit4u!123')
    
    #cursor = connection.cursor()
    #query = "UPDATE AGOL_TEST_PY_2_evw SET Taxonomy = 'Obla dee' WHERE OBJECTID = 484
    # execute the query and read to a dataframe in Python
    #data = pd.read_sql(query, connection)
    #print(data)

    #registration_id = sql.GetRegistrationId(connection, 'AGOL_TEST_PY_2')

    #sql.ExtractChanges(connection, registration_id, 'AGOL_TEST_PY_2', 0)
    #ids = GetSdeStateIdsSinceId(connection, 'AGOL_TEST_PY_2', 'DEFAULT', 0)
    
    #deletes = sql.GetDeletes(connection, registration_id, 0)
    #sql.SdeObjectIdsToGlobalIds(connection, deletes, 'AGOL_TEST_PY_2', registration_id)
    
    #connection.close()

if __name__ == '__main__':
    test()
