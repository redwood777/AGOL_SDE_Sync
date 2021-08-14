##WELCOME TO THE LEJANZ AGOL/SDE SYNC TOOL
##Terminology:
##    service: the featureclass or feature service
##    sync: a pair of services registered with this tool
##    deltas: edits extracted from or applied to a service
    
import json
import copy
#import config
#import sde_functions as sde
#import agol_functions as agol
import ui_functions as ui

def LoadConfig():
    try:
        import config
    except:
        print('Error loading config.')
        return False
        #TODO: make config builder?
    
    return config
        

def LoadSyncs():
    #loads json file containing set up syncs
    try:
        syncs_file = open('syncs.json', 'r')
    except:
        print('No sync file.')
        return None

    try:
        syncs = json.load(syncs_file)
    except:
        print('Invalid sync file')
        return None

    syncs_file.close()
    return syncs

def WriteSyncs(syncs):
    #writes sync.json with data in syncs
    syncs_file = open('syncs.json', 'w')
    json.dump(syncs, syncs_file, indent=4)
    syncs_file.close()

def ExtractChanges(service, serverGen, cfg):
    #wrapper for SQL/AGOL extract changes functions
    if(service['type'] == 'SDE'):
        connection = sql.Connect(service['hostname'], service['database'], cfg.SQL_username, cfg.SQL_password)
        registration_id = sql.GetRegistrationId(connection, service['featureclass'])
        return sql.ExtractChanges(connection, registration_id, service['featureclass'], serverGen)

def ApplyEdits(service, cfg, deltas):
    #wrapper for SQL/AGOL extract changes functions
    
    if(service['type'] == 'SDE'):
        connection = sql.Connect(service['hostname'], service['database'], cfg.SQL_username, cfg.SQL_password)
        registration_id = sql.GetRegistrationId(connection, service['featureclass'])
        sql.ApplyEdits(connection, registration_id, service['featureclass'], deltas)
        #print(sql.GetStatesSince(connection, 0))  

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
##    cfg = LoadConfig()
##    syncs = LoadSyncs()
##
##    menu = [s['name'] for s in syncs]
##    menu.append('Create new')
##    choice = ui.Options('Select sync:', menu)
##
##    if (choice == (len(menu))):
##        #TODO: make create new builder
##        print(None)
##    else:
##        sync = syncs[choice - 1]
##
##    out = ExtractChanges(sync['first'], sync['first_servergen'], cfg)
##    ApplyEdits(sync['second'], cfg, out)
    
    

    
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


    #config = LoadConfig()
    #print(config.name)
    #deltas, deltas2 = ResolveConflicts(deltas, deltas2)
    ui.ResolveConflicts(deltas, deltas2, 'ONE', 'TWO')

    #print(LoadSyncs())

    #connection = sql.Connect('inpredwgis2', 'REDWTest', 'REDW_Python', 'Benefit4u!123')
    #sql.GetSyncs(connection, 'AGOL_TEST_PY_2')
    
    
    #connection.close()

if __name__ == '__main__':
    test()
