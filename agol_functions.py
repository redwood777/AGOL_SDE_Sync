import json
import requests
import ui_functions as ui
import time

def GetToken(url, username, password):
    #returns token for use with further requests

    ui.Debug('Getting AGOL token...\n', 2)
    
    url = url + '/sharing/generateToken'
    payload  = {'username' : username,'password' : password,'referer' : 'www.arcgis.com','f' : 'json' }

    r = requests.post(url, data=payload)

    response = json.loads(r.content)
    
    try:
        token = response['token']
    except:
        print('No token returned!')
        print(response)

    return token


def CreateUrl(base_url, params):
    base_url += '?'
    
    for k,v in params.items():
        base_url += '{}={}&'.format(k,v)
        
    base_url += 'f=json'
    
    ui.Debug('URL: {}'.format(base_url), 3)
    
    return base_url
                
                

def ApiCall(url, data, token): #, serverGen):
    #performs async rest api call 

    ui.Debug('Sending API request...\n', 2)

    url = CreateUrl(url, data)
    response = requests.post(url)

    url = json.loads(response.content)["statusUrl"]  
    data  = {'token': token}
    url = CreateUrl(url, data)

    while True:
        time.sleep(3)

        ui.Debug('Checking status URL...', 2)
        response = requests.post(url)
        content = json.loads(response.content)
        ui.Debug('Status: {}'.format(content['status']), 2)
        
        if (content["status"] != 'Pending'):
            break

    if content['status'] == 'Failed':
        print(content['error'])
        return

    else:
        ui.Debug('\nGetting result...\n', 2)
        
        url = content['resultUrl']
        url = CreateUrl(url, data)

        response = requests.post(url)
        content = json.loads(response.content)
        #print(json.dumps(content, indent=4))

    return content

def CheckService(base_url, layer, token): #, serverGen):
    #returns None if issue with service
    #returns False if service is missing capabilities
    #returns True, serverGen if service is set up correctly

    ui.Debug('Checking AGOL service capabilities...\n', 1)

    data  = {'token': token,
            'returnUpdates': True}

    url = CreateUrl(base_url, data)
    
    response = requests.post(url)

    if(response.status_code !=  200):
        print('HTTP Error code: {}'.format(response.status_code))
        return False, None

    try: 
        content = json.loads(response.content)
        capabilities = content['capabilities']
    except:
        print('Error parsing response!')
        return False, None

    capabilities = capabilities.lower()
    required = ['update', 'changetracking', 'create', 'delete', 'update', 'editing']

    for req in required:
        if not req in capabilities:
            print('Missing capability: {}'.format(req))
            return False, None

    serverGens = content["changeTrackingInfo"]['layerServerGens']
    serverGen = [g for g in serverGens if g['id'] == layer]

    try:
        serverGen = serverGen[0]
    except:
        print('Layer {} does not exist'.format(layer))
        return False, None

    return True, serverGen



def ExtractChanges(url, layer, serverGen, token):
    #extracts changes since specified serverGen and returns them as an object

    ui.Debug('Extracting changes from AGOL...\n', 1)

    data  = {'token': token,
            'layers': [layer],
            'returnInserts': 'true',
            'returnUpdates': 'true',
            'returnDeletes': 'true',
            'layerServerGens': json.dumps([serverGen]),
            'dataFormat': 'json'}

               
    url = base_url + '/extractChanges'
    
    response = ApiCall(url, data, token)

    return response['edits'][0]['features']

def ApplyEdits(url, layer, token, deltas):
    #applies edits to service, returns success boolean

    ui.Debug('Applying edits to AGOL...\n', 1)
    
    deltas['id'] = layer

    #print(json.dumps(deltas, indent=4))

    data = {'token': token,
            'edits': [deltas],
            'useGlobalIds': 'true'}

    url += '/applyEdits'

    url = CreateUrl(url, data)

    response = requests.post(url) #, json={'edits': deltas})

    if(response.status_code != 200):
        print('HTTP Error code: {}'.format(response.status_code))
        return False

    print(response.content)

    try:
        response = json.loads(response.content)[0]
    except:
        print('Invalid response')
        return False

    success = True

    for results in ['addResults', 'updateResults', 'deleteResults']:
        if (results in response.keys()):
            for result in response[results]:
                if not result['success']:
                    print(result['error'])
                    success = False

    if(not success):
        print('Error: {}'.format(response[0]['error']))
        return False
    
    return True

base_url = 'https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/REDW_AGOL_PythonSyncTest_py/FeatureServer'

deltas = {
    "deleteIds": [], 
    "adds": [], 
    "updates": [
        {
            "geometry": {
                "rings": [
                    [
                        [
                            400256.578804272, 
                            4640459.73021187
                        ], 
                        [
                            400343.341193316, 
                            4640363.63900759
                        ], 
                        [
                            400200.17907481, 
                            4640372.7397159
                        ], 
                        [
                            400256.578804272, 
                            4640459.73021187
                        ]
                    ]
                ]
            }, 
            "attributes": {
                "CreateUser": "REDW_Python", 
                "GlobalID": "A6F21C34-7B36-48ED-9F16-EDB58DB3CE5C", 
                "UTM_Zone": "10",
                "Species_ID": 'BEOBOO',
                "Taxonomy": 'MOBETTAS'
            }
        }
    ]
}

#token = GetToken('https://nps.maps.arcgis.com', 'REDW_Python', 'Benefit4u!')
#print(CheckService(base_url, 0, token))
#serverGens = GetServerGen(base_url, token)
#serverGens = [{'serverGen': 57940165, 'id': 0, 'minServerGen': 57939871}]
#print(serverGens)
#deltas = ExtractChanges(base_url, 0, serverGens, token)
#deltas['updates'] = deltas['adds']
#deltas['adds'] = []
#print(json.dumps(deltas, indent=4))
#print(ApplyEdits(base_url, 0, token, deltas))
