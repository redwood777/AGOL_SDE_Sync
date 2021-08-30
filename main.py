##WELCOME TO THE LEJANZ AGOL/SDE SYNC TOOL
##Terminology:
##    service: the featureclass or feature service
##    sync: a pair of services registered with this tool
##    deltas: edits extracted from or applied to a service
    
import json
import copy
#import sde_functions as sde
import agol_functions as agol
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
    try:
        json.dumps(syncs)
    except:
        print("Invalid syncs!")
        return
    
    syncs_file = open('syncs.json', 'w')
    json.dump(syncs, syncs_file, indent=4)
    syncs_file.close()

def ExtractChanges(service, serverGen, cfg):
    #wrapper for SQL/AGOL extract changes functions
    if(service['type'] == 'SDE'):
        connection = sde.Connect(cfg.SQL_hostname, service['database'], cfg.SQL_username, cfg.SQL_password)
        registration_id = sde.GetRegistrationId(connection, service['featureclass'])
        deltas = sde.ExtractChanges(connection, registration_id, service['featureclass'], service['globalIds'], serverGen)
        connection.close()
    
    elif(service['type'] == 'AGOL'):
        token = agol.GetToken(cfg.AGOL_url, cfg.AGOL_username, cfg.AGOL_password)
        ready, newServerGen = agol.CheckService(service['serviceUrl'], service['layerId'], token)

        if not ready:
            return

        deltas = agol.ExtractChanges(service['serviceUrl'], service['layerId'], serverGen, token)

    return deltas

def ApplyEdits(service, cfg, deltas):
    #wrapper for SQL/AGOL extract changes functions
    
    if(service['type'] == 'SDE'):
        #connect
        connection = sde.Connect(cfg.SQL_hostname, service['database'], cfg.SQL_username, cfg.SQL_password)
        
        #get registration id and extract changes
        registration_id = sde.GetRegistrationId(connection, service['featureclass'])
        sde.ApplyEdits(connection, registration_id, service['featureclass'], deltas)

        #commit changes
        connection.commit()

        #get new state id
        state_id = sde.GetCurrentStateId(connection)

        #close connection
        connection.close()
        return state_id

    elif(service['type'] == 'AGOL'):
        token = agol.GetToken(cfg.AGOL_url, cfg.AGOL_username, cfg.AGOL_password)
        ready = agol.CheckService(service['serviceUrl'], service['layerId'], token)

        if not ready:
            return

        if not agol.ApplyEdits(service['serviceUrl'], service['layerId'], token, deltas):
            return

        ready, newServerGen = agol.CheckService(service['serviceUrl'], service['layerId'], token)

        return newServerGen
        

def main():
    #load config
    cfg = LoadConfig()
    ui.SetLogLevel(cfg)

    #load syncs
    syncs = LoadSyncs()

    if(syncs == None):
        exit()

    #prompt user to select sync
    menu = [s['name'] for s in syncs]
    menu.append('Create new')
    choice = ui.Options('Select sync:', menu, allow_filter=True)

    if (choice == (len(menu))):
        sync = ui.CreateNewSync(cfg)
        syncs.append(sync)
        WriteSyncs(syncs)
        exit()
    else:
        sync = syncs[choice - 1]

    #Extract changes from both services
    first_deltas = ExtractChanges(sync['first'], sync['first']['servergen'], cfg)
    second_deltas = ExtractChanges(sync['second'], sync['second']['servergen'], cfg)
    
    #reconcile changes
    first_deltas, second_deltas = ui.ResolveConflicts(first_deltas, second_deltas, 'PY_2', 'PY_3')
    
    #Apply edits
    second_servergen = ApplyEdits(sync['second'], cfg, first_deltas)
    first_servergen = ApplyEdits(sync['first'], cfg, second_deltas)

    #Update servergens
    syncs[choice - 1]['first']['servergen'] = first_servergen
    syncs[choice - 1]['second']['servergen'] = second_servergen
    
    WriteSyncs(syncs)

    print('Done!')
    
    
    

def test():
    cfg = LoadConfig()
    ui.SetLogLevel(cfg)
    ui.Debug('gamer', 0)
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
    #ui.ResolveConflicts(deltas, deltas2, 'ONE', 'TWO')

    #print(LoadSyncs())

    #connection = sql.Connect('inpredwgis2', 'REDWTest', 'REDW_Python', 'Benefit4u!123')
    #sql.GetSyncs(connection, 'AGOL_TEST_PY_2')
    
    
    #connection.close()

if __name__ == '__main__':
    main()
