##WELCOME TO THE LEJANZ AGOL/SDE SYNC TOOL

## Version:                       1.3
## Milestone:                     Remove case sensitivity
## Release Date:                  09/12/2021
## Notable corrections needeed: 
## Delete statement for SDE feature needs quotes on GUID; 

##Terminology:
##    service: the featureclass or feature service
##    sync: a pair of services registered with this tool
##    deltas: edits extracted from or applied to a service
    
import json
import copy
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
    ui.Debug('Loading config...', 0)
    try:
        import config
    except:
        print('Error loading config!\n')
        return False
        #TODO: make config builder?

    ui.Debug('Done.\n', 0, indent=4)
    
    return config
        

def LoadSyncs():
    #loads json file containing set up syncs
    ui.Debug('Loading syncs...', 2)
    
    try:
        syncs_file = open('syncs.json', 'r')
    except:
        print('No syncs.json file found!')
        syncs = []

    try:
        syncs = json.load(syncs_file)
    except:
        print('Invalid sync file!')
        syncs = []

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

            #get hostname
            hostname = raw_input('Enter SDE hostname (i.e. inpredwgis2)')

            #get database name
            database = raw_input('Enter SDE database name (i.e. redw):')

            #get featureclass name
            fcName = raw_input('Enter featureclass name:')

            print('')

            #check that featureclass exists in sde table registry 
            connection = sde.Connect(hostname, database, cfg.SQL_username, cfg.SQL_password)

            if(sde.CheckFeatureclass(connection, fcName)):
                
                #get current information
                stateId = sde.GetCurrentStateId(connection)
                globalIds = sde.GetGlobalIds(connection, fcName)

                ui.Debug('Featureclass added successfully!\n', 1)

                service = {'servergen': {'stateId': stateId, 'globalIds': globalIds},
                           'type': 'SDE',
                           'featureclass': fcName,
                           'hostname': hostname,
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
            ready, serverGen, srid = agol.CheckService(url, layerId, token)

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

def CleanAttributes(dict_in):
    dict_in = {k.lower(): v for k, v in dict_in.items() if v is not None}

    remove_keys = ['sde_state_id', 'object_id']
    dict_in = {k: v for k, v in dict_in.items() if k not in remove_keys}

    return dict_in
    
def CleanDelta(dict_in, srid):
    #removes nulls, turns all keys to lower case
    dict_in['attributes'] = CleanAttributes(dict_in['attributes'])
    dict_in['geometry']['spatialReference'] = {'wkid': srid}

    return dict_in

def ExtractChanges(service, serverGen, cfg):
    #wrapper for SQL/AGOL extract changes functions
    if(service['type'] == 'SDE'):
        ImportSDE()
        
        connection = sde.Connect(service['hostname'], service['database'], cfg.SQL_username, cfg.SQL_password)
        datatypes = sde.GetDatatypes(connection, service['featureclass'])
        srid = sde.GetSRID(connection, service['featureclass'])
        
        deltas = sde.ExtractChanges(connection, service['featureclass'], serverGen['globalIds'], serverGen['stateId'], datatypes)

        data = {'connection': connection, 'datatypes': datatypes} 
    
    elif(service['type'] == 'AGOL'):
        ImportAGOL()
        
        token = agol.GetToken(cfg.AGOL_url, cfg.AGOL_username, cfg.AGOL_password)
        ready, newServerGen, srid = agol.CheckService(service['serviceUrl'], service['layerId'], token)

        if not ready:
            return None

        deltas = agol.ExtractChanges(service['serviceUrl'], service['layerId'], serverGen, token) 

        data = {'token': token}

    print json.dumps(deltas, indent=4)

    for add in deltas['adds']:
        add = CleanDelta(add, srid)
        
    for update in deltas['updates']:
        update = CleanDelta(update, srid)

    print json.dumps(deltas, indent=4)

    return deltas, data

def ApplyEdits(service, cfg, deltas, data=None):
    #wrapper for SQL/AGOL extract changes functions
    
    if(service['type'] == 'SDE'):
        ImportSDE()
        #connect

        if data == None:
            connection = sde.Connect(service['hostname'], service['database'], cfg.SQL_username, cfg.SQL_password)
            datatypes = sde.GetDatatypes(connection, service['featureclass'])
        else:
            connection = data['connection']
            datatypes = data['datatypes']
        
        #get registration id and extract changes
        #registration_id = sde.GetRegistrationId(connection, service['featureclass'])
            
        if not sde.ApplyEdits(connection, service['featureclass'], deltas, datatypes):
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
            ready, gen, srid = agol.CheckService(service['serviceUrl'], service['layerId'], token)

            if not ready:
                return False
        else:
            token = data['token']

        if not agol.ApplyEdits(service['serviceUrl'], service['layerId'], token, deltas):
            return False

        ready, newServerGen, srid = agol.CheckService(service['serviceUrl'], service['layerId'], token)

        return newServerGen
        

def main(): 
    #load config
    cfg = LoadConfig()
    ui.SetLogLevel(cfg)

    #load syncs
    syncs = LoadSyncs()

    while True:

        #prompt user to select sync
        syncNames = [s['name'] for s in syncs]
        menu = syncNames[:]
        menu.insert(0,'Create new')
        menu.insert(1,'Delete sync')
        menu.insert(2,'Exit')
        choice = ui.Options('Select sync:', menu, allow_filter=True)

        if (choice == 1):
            sync = CreateNewSync(cfg)
            syncs.append(sync)
            WriteSyncs(syncs)
            print('Sync created!\n')

        elif (choice == 2):
            deleteIndex = ui.Options('Choose sync to delete', syncNames)
            syncs.pop(deleteIndex - 1)
            WriteSyncs(syncs)
            print('Sync deleted!\n')

        elif (choice == 3):
            print('HEHEHEHEHEHE')
            return
            
        else:
            sync = syncs[choice - 4]

            #Extract changes from both services
            first_deltas, first_data = ExtractChanges(sync['first'], sync['first']['servergen'], cfg)
            second_deltas, second_data = ExtractChanges(sync['second'], sync['second']['servergen'], cfg)

            if first_deltas == None or second_deltas == None:
                print('Failed.\n')
                continue
            
            #reconcile changes
            first_deltas, second_deltas = ui.ResolveConflicts(first_deltas, second_deltas, 'PY_2', 'PY_3')
            
            #Apply edits
            second_servergen = ApplyEdits(sync['second'], cfg, first_deltas, data=second_data)
            first_servergen = ApplyEdits(sync['first'], cfg, second_deltas, data=first_data)

            #check success
            if (second_servergen and first_servergen):    
                #Update servergens
                syncs[choice - 4]['first']['servergen'] = first_servergen
                syncs[choice - 4]['second']['servergen'] = second_servergen
                
                WriteSyncs(syncs)

                print('Success!\n')
              
            else:
                print('Failed.\n')
    
    
    
first_deltas = {
    "deleteIds": [], 
    "adds": [], 
    "updates": [
        {
            "geometry": {
                "rings": [
                    [
                        [
                            400776.672250849, 
                            4640112.25815286
                        ], 
                        [
                            400759.700596652, 
                            4640104.36435203
                        ], 
                        [
                            400729.907206961, 
                            4640073.74643086
                        ], 
                        [
                            400711.748572683, 
                            4640108.42937512
                        ], 
                        [
                            400615.261475172, 
                            4640126.84045026
                        ], 
                        [
                            400681.167664727, 
                            4640174.64573512
                        ], 
                        [
                            400714.206264933, 
                            4640185.52063968
                        ], 
                        [
                            400747.460103052, 
                            4640182.51656634
                        ], 
                        [
                            400755.753649996, 
                            4640146.59620403
                        ], 
                        [
                            400761.674042239, 
                            4640144.62274921
                        ], 
                        [
                            400782.022575571, 
                            4640195.51494364
                        ], 
                        [
                            400768.778450053, 
                            4640135.15019929
                        ], 
                        [
                            400800.254065003, 
                            4640171.50286492
                        ], 
                        [
                            400773.909449216, 
                            4640126.86170195
                        ], 
                        [
                            400776.672250849, 
                            4640112.25815286
                        ]
                    ]
                ], 
                "spatialReference": {
                    "wkid": 26910
                }
            }, 
            "attributes": {
                "tmp_disturbmax": "L", 
                "visit_type": "Inventory", 
                "utm_zone": "10", 
                "editdate": 1631472564095, 
                "taxonomy": "Obla dee", 
                "polyhistory": "2008-P-0008;", 
                "visit_year": 2008, 
                "location_general": "Tolowa Dunes State Park", 
                "native_coverclass_range": "999", 
                "xfield": 400756, 
                "tmp_disturbpotential": "N2", 
                "mappedacres": 0.29111232, 
                "management_actions": "No Treatment", 
                "species_id": "HYPCAL", 
                "yfield": 4640125, 
                "location_name": "Yontocket", 
                "ruleid": 1, 
                "projectname": "Tolowa St. John's Worts", 
                "potentialdisturbance": "HYPCAL-N2", 
                "edituser": "REDW_Python", 
                "percenttreated": 0, 
                "utm_datum": "NAD83", 
                "yearspeciestreat": "2008HYPCALInventory", 
                "tmp_disturbmin": "N", 
                "gpsreceiver": "Trimble GeoXT", 
                "coverclass_range": "999", 
                "coverclass": 8, 
                "globalid": "0600494B-D048-4F1C-8063-0B1A5EDD7C0F", 
                "source_mapping": "GPS, Trimble GeoXT", 
                "createdate": 1630364185790, 
                "densityadjustedacres": 0, 
                "cover_density": "Low", 
                "zz_feature_id": 484, 
                "treatment_specific": "Inventory", 
                "zz_replace_geom": "No", 
                "visitdatelast": "20080530", 
                "quad": "Smith River", 
                "location_broad": "A-North", 
                "county": "Del Norte", 
                "native_coverclass": 111, 
                "collectdate": 0, 
                "previous_treat": "2001", 
                "recorder_name": "Laura Julian", 
                "disturbcategory": "HYPCAL-L", 
                "date_fyall": "20080530", 
                "tmp_disturbcurrent": "L", 
                "location_specific": "TDSP - North", 
                "visit_date": "20080530", 
                "date_mapped": "20080530", 
                "treatcategory": "Inventory", 
                "starttime": "1200", 
                "createuser": "REDW_Python", 
                "distribution": "Continuous", 
                "tmp_charlocation": 7, 
                "phenology": "vegetative", 
                "datemonitor": "20080530"
            }
        }
    ]
}



if __name__ == '__main__':
    main()
