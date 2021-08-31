##WELCOME TO THE LEJANZ AGOL/SDE SYNC TOOL
##Terminology:
##    service: the featureclass or feature service
##    sync: a pair of services registered with this tool
##    deltas: edits extracted from or applied to a service
    
import json
import copy
#import sde_functions as sde
#import agol_functions as agol
import ui_functions as ui

sde = None
agol = None

def ImportSDE():
    global sde
    if sde == None:
        ui.Debug('Loading SDE functions...', 2)
        import sde_functions as sde
        ui.Debug('Done.\n', 2, indent=4)

def ImportAGOL():
    global agol
    if agol == None:
        ui.Debug('Loading AGOL functions...', 2)
        import agol_functions as agol
        ui.Debug('Done.\n', 2, indent=4)

def LoadConfig():
    ui.Debug('Loading config...', 2)
    try:
        import config
    except:
        print('Error loading config!\n')
        return False
        #TODO: make config builder?

    ui.Debug('Done.\n', 2, indent=4)
    
    return config
        

def LoadSyncs():
    #loads json file containing set up syncs
    ui.Debug('Loading syncs...', 2)
    
    try:
        syncs_file = open('syncs.json', 'r')
    except:
        print('No syncs.json file found!')
        return None

    try:
        syncs = json.load(syncs_file)
    except:
        print('Invalid sync file!')
        return None

    syncs_file.close()
    ui.Debug('Done.\n', 2, indent=4)
    return syncs

def WriteSyncs(syncs):
    #writes sync.json with data in syncs
    ui.Debug('Updating syncs...', 2)
    
    try:
        json.dumps(syncs)
    except:
        print("Invalid syncs!")
        return
    
    syncs_file = open('syncs.json', 'w')
    json.dump(syncs, syncs_file, indent=4)
    syncs_file.close()

    ui.Debug('Done.\n', 2, indent=4)

def CreateNewSync(cfg):
    #UI to create a new sync

    print('Please ensure that the two copies you are setting up for sync are currently identical!\nThis tool may not function correctly otherwise!\n')

    name = raw_input('Please enter a name for this sync:')

    numbers = ['first', 'second']

    sync = {'name': name, 'first': {}, 'second': {}}
    
    i = 0
    while(i < 2): 
        print('\nEnter the details for your {} service:\n').format(numbers[i])

        types = ['SDE', 'AGOL']

        serviceType = ui.Options('Select service type:', types)

        if(serviceType == 1):
            #for SDE services

            ImportSDE()

            #get database name
            database = raw_input('Enter SDE database name (i.e. redw):')

            #get featureclass name
            fcName = raw_input('Enter featureclass name:')

            print('')

            #check that featureclass exists in sde table registry 
            connection = sde.Connect(cfg.SQL_hostname, database, cfg.SQL_username, cfg.SQL_password)

            if(sde.CheckFeatureclass(connection, fcName)):
                
                #get current information
                stateId = sde.GetCurrentStateId(connection)
                globalIds = sde.GetGlobalIds(connection, fcName)

                ui.Debug('Featureclass added successfully!\n', 1)

                service = {'servergen': {'stateId': stateId, 'globalIds': globalIds},
                           'type': 'SDE',
                           'featureclass': fcName,
                           'database': database}
            else:
                continue
            
        else:
            #for AGOL services
            ImportAGOL()
            
            #get service details
            url = raw_input('Enter service url:')
            layerId = int(raw_input('Enter service layer id:'))

            #check that service is set up correctly
            token = agol.GetToken(cfg.AGOL_url, cfg.AGOL_username, cfg.AGOL_password)
            ready, serverGen = agol.CheckService(url, layerId, token)

            if not ready:
                continue

            ui.Debug('Feature service added successfully!\n', 1)

            service = {'type': 'AGOL',
                       'serviceUrl': url,
                       'layerId': layerId,
                       'servergen': serverGen}

        sync[numbers[i]] = service
        i = i + 1

    return sync

def ExtractChanges(service, serverGen, cfg):
    #wrapper for SQL/AGOL extract changes functions
    if(service['type'] == 'SDE'):
        ImportSDE()
        
        connection = sde.Connect(cfg.SQL_hostname, service['database'], cfg.SQL_username, cfg.SQL_password)
        #registration_id = sde.GetRegistrationId(connection, service['featureclass'])
        #if registration_id == None:
        #    connection.close()
        #    return False
        
        deltas = sde.ExtractChanges(connection, service['featureclass'], serverGen['globalIds'], serverGen['stateId'])

        data = {'connection': connection}
        
    
    elif(service['type'] == 'AGOL'):
        ImportAGOL()
        
        token = agol.GetToken(cfg.AGOL_url, cfg.AGOL_username, cfg.AGOL_password)
        ready, newServerGen = agol.CheckService(service['serviceUrl'], service['layerId'], token)

        if not ready:
            return False

        deltas = agol.ExtractChanges(service['serviceUrl'], service['layerId'], serverGen, token)

        data = {'token': token}

    return deltas, data

def ApplyEdits(service, cfg, deltas, data=None):
    #wrapper for SQL/AGOL extract changes functions
    
    if(service['type'] == 'SDE'):
        ImportSDE()
        #connect

        if data == None:
            connection = sde.Connect(cfg.SQL_hostname, service['database'], cfg.SQL_username, cfg.SQL_password)
        else:
            connection = data['connection']
        
        #get registration id and extract changes
        #registration_id = sde.GetRegistrationId(connection, service['featureclass'])
            
        if not sde.ApplyEdits(connection, service['featureclass'], deltas):
            return False

        #commit changes
        connection.commit()

        #get new state id and global ids
        state_id = sde.GetCurrentStateId(connection)
        globalIds = sde.GetGlobalIds(connection, service['featureclass'])

        #close connection
        connection.close()

        return {'stateId': state_id, 'globalIds': globalIds}

    elif(service['type'] == 'AGOL'):
        ImportAGOL()

        if data == None:
            token = agol.GetToken(cfg.AGOL_url, cfg.AGOL_username, cfg.AGOL_password)
            ready = agol.CheckService(service['serviceUrl'], service['layerId'], token)

            if not ready:
                return False
        else:
            token = data['token']

        if not agol.ApplyEdits(service['serviceUrl'], service['layerId'], token, deltas):
            return False

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
    syncNames = [s['name'] for s in syncs]
    menu = syncNames[:]
    menu.append('Create new')
    menu.append('Delete sync')
    choice = ui.Options('Select sync:', menu, allow_filter=True)

    if (choice == (len(menu) - 1)):
        sync = CreateNewSync(cfg)
        syncs.append(sync)
        WriteSyncs(syncs)
        print('Sync created! Exiting.')

    elif (choice == len(menu)):
        deleteIndex = ui.Options('Choose sync to delete', syncNames)
        syncs.pop(deleteIndex - 1)
        WriteSyncs(syncs)
        print('Sync deleted! Exiting')
        
    else:
        sync = syncs[choice - 1]

        #Extract changes from both services
        first_deltas, first_data = ExtractChanges(sync['first'], sync['first']['servergen'], cfg)
        second_deltas, second_data = ExtractChanges(sync['second'], sync['second']['servergen'], cfg)
        
        #reconcile changes
        first_deltas, second_deltas = ui.ResolveConflicts(first_deltas, second_deltas, 'PY_2', 'PY_3')
        
        #Apply edits
        second_servergen = ApplyEdits(sync['second'], cfg, first_deltas, data=second_data)
        first_servergen = ApplyEdits(sync['first'], cfg, second_deltas, data=first_data)

        #check success
        if (second_servergen and first_servergen):    
            #Update servergens
            syncs[choice - 1]['first']['servergen'] = first_servergen
            syncs[choice - 1]['second']['servergen'] = second_servergen
            
            WriteSyncs(syncs)

            print('Success!')
            return

        print('Failed.')
    
    
    

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
