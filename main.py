import json
import copy
#import config
#import sql_functions as sql
#import json_functions as jsn

def GetGlobalIds(dict_in):
    #pulls global ids from adds or updates dictionary, returns as set
    return {add['attributes']['GlobalID'] for add in dict_in}

def LoadConfig():
    try:
        import config
        return config
    except:
        print('Error loading config.')
        return False
        #TODO: make config builder?

def Options(prompt, menu):
    i = 1
    print(prompt)
    for item in menu:
        print('  {}. {}'.format(i, item))
        i+=1
    return input('Enter selection:')

def ResolveConflicts(SDE_deltas, AGOL_deltas):
    #Finds all conflicting edits. Resolves conflicts by user input. Returns revised SDE_deltas and AGOL_deltas

    print('Checking for conflicts...\n')
    #From here on, we will work only with global ids
    #SDE_added = GetGlobalIds(SDE_deltas['adds'])
    #AGOL_added = GetGlobalIds(AGOL_deltas['adds'])
    SDE_updated = GetGlobalIds(SDE_deltas['updates'])
    AGOL_updated = GetGlobalIds(AGOL_deltas['updates'])
    
    #remove deletes that have already occured in destination, and store as a set
    SDE_deleted = set(SDE_deltas['deleteIds']).difference(AGOL_deltas['deleteIds'])
    AGOL_deleted = set(AGOL_deltas['deleteIds']).difference(SDE_deltas['deleteIds'])
    
    #print("SDE_deleted:", SDE_deleted)
    #print("AGOL_deleted:", AGOL_deleted)
    #print("SDE_updated:", SDE_updated)
    #print("AGOL_updated:", AGOL_updated)


    #find update/delete conflictions
    AGOL_updated_SDE_deleted = AGOL_updated.intersection(SDE_deleted)
    SDE_updated_AGOL_deleted = SDE_updated.intersection(AGOL_deleted)

    #find update/update conflictions
    both_updated = AGOL_updated.intersection(SDE_updated)

    #print("AGOL_updated_SDE_deleted:", AGOL_updated_SDE_deleted)
    #print("SDE_updated_AGOL_deleted:", SDE_updated_AGOL_deleted)
    #print("both_updated:", both_updated)
    
    #print(json.dumps(AGOL_deltas, indent=4))
    #print(json.dumps(SDE_deltas, indent=4))

    #calculate sum of conflicts
    total_conflicts = len(AGOL_updated_SDE_deleted) + len(SDE_updated_AGOL_deleted) + len(both_updated)

    if(total_conflicts < 1):
        print('No conflicts found.\n')
    else:
        #display sum of conflicts
        #prompt user to resolve all one way, resolve manually, show more info, or cancel
        prompt = '{} conflicts found. Choose conflict resolution:'.format(total_conflicts)
        menu = ['Prioritize AGOL Changes', 'Prioritize SDE Changes', 'Choose for each conflict', 'More info', 'Cancel']
        choice = Options(prompt, menu)

        #in update/delete conflicts, update will either become add or be removed
        AGOL_updated -= AGOL_updated_SDE_deleted
        SDE_updated -= SDE_updated_AGOL_deleted
        
        #print(choice)
        #sets to store global ids of objects being moved from updates to adds
        AGOL_new_adds = set()
        SDE_new_adds = set()

        #if all in favor of AGOL:
        if (choice == 1):
            #move AGOL_updated_SDE_deleted from AGOL_updates to AGOL_adds
            AGOL_new_adds = AGOL_updated_SDE_deleted
            
            #remove AGOL_updated_SDE_deleted from SDE_deletes
            SDE_deleted -= AGOL_updated_SDE_deleted

            #remove both_updated from SDE updated
            SDE_updated -= both_updated

        #same for all in favor of SDE:
        if (choice == 2):
            SDE_new_adds = SDE_updated_AGOL_deleted         
            AGOL_deleted -= SDE_updated_AGOL_deleted
            AGOL_updated -= both_updated

        #if manual:
        if (choice == 3):
            #run through all conflict lists, print out conflict, prompt to resolve in favor of AGOL or SDE
            menu = ['Keep update from AGOL', 'Keep delete from SDE']
            #for update/delete conflicts:
            for conflict in AGOL_updated_SDE_deleted:
                prompt = 'Object "{}" was updated in AGOL and deleted in SDE. Choose:'.format(conflict)
                choice = Options(prompt, menu)
                
                #if in favor of update: update -> add, delete removed
                if (choice == 1):
                    AGOL_new_adds.add(conflict)
                    SDE_deleted.remove(conflict)
                #if in favor of delete: update removed (already done above)

            menu = ['Keep delete from AGOL', 'Keep update from SDE']
            
            for conflict in SDE_updated_AGOL_deleted:
                prompt = 'Object "{}" was deleted in AGOL and updated in SDE. Choose:'.format(conflict)
                choice = Options(prompt, menu)
                
                #if in favor of update: update -> add, delete removed
                if (choice == 2):
                    SDE_new_adds.add(conflict)
                    AGOL_deleted.remove(conflict)
                #if in favor of delete: update removed (already done above)
                

            #for update/update conflicts:
            menu = ['Keep update from AGOL', 'Keep update from SDE']

            for conflict in both_updated:
                prompt = 'Object "{}" was updated in both AGOL and SDE. Choose:'.format(conflict)
                choice = Options(prompt, menu)
                #losing update removed
                if(choice == 1):
                    SDE_updated.remove(conflict)
                elif(choice == 2):
                    AGOL_updated.remove(conflict)
                
        #build new json objects:

        #lists to store new updates
        revisedSdeUpdates = []
        revisedAgolUpdates = []

        #run through old updates and add them to new updates or adds
        for update in SDE_deltas['updates']:
            GUID = update['attributes']['GlobalID']
            if GUID in SDE_updated:
                revisedSdeUpdates.append(update)
            if GUID in SDE_new_adds:
                SDE_deltas['adds'].append(update)

        for update in AGOL_deltas['updates']:
            GUID = update['attributes']['GlobalID']
            if GUID in AGOL_updated:
                revisedAgolUpdates.append(update)
            if GUID in AGOL_new_adds:
                AGOL_deltas['adds'].append(update)

        #overwrite old updates
        AGOL_deltas['updates'] = revisedAgolUpdates
        SDE_deltas['updates'] = revisedSdeUpdates

    #overwrite old deletes (even if no conflicts, because deletes are checked for uniqueness above)
    AGOL_deltas['deleteIds'] = list(AGOL_deleted)
    SDE_deltas['deleteIds'] = list(SDE_deleted)

    #print("SDE_deleted:", SDE_deleted)
    #print("AGOL_deleted:", AGOL_deleted)
    #print("SDE_updated:", SDE_updated)
    #print("AGOL_updated:", AGOL_updated)
    #print("SDE_new_adds:", SDE_new_adds)
    #print("AGOL_new_adds:", AGOL_new_adds)
    
    #print(json.dumps(AGOL_deltas, indent=4))
    #print(json.dumps(SDE_deltas, indent=4))
    
       
    return SDE_deltas, AGOL_deltas

def main():
    #load config
    cfg = LoadConfig()
    
    #get database, FC name
    if(cfg):
        menu = [d['name'] for d in cfg.SQL_databases]
        menu.append('Create new')
        choice = Options('Select database:', menu)
    
    #check if FC has been set up before (cached in SQL table?)
        #if so, get last serverGen/SDE_STATE_ID
        #if not, ask for service URL and cache
    
    #connect to database
    #UID = 'REDW_Python'
    #PWD = 'Benefit4u!123'
    
    #cnxn = sql.Connect(server, database, UID, PWD)
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

    config = LoadConfig()
    print(config.name)
    #deltas, deltas2 = ResolveConflicts(deltas, deltas2)
    #ResolveConflicts(deltas, deltas2)
    #print(Options('select', ['a', 'b', 'c']))
 
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
    main()
