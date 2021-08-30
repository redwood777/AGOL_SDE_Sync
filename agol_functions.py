import json
#import pandas as pd
import requests
import ui_functions as ui
import time

def GetToken(url, username, password):
    #returns token for use with further requests
    
    url = url + '/sharing/generateToken'
    payload  = {'username' : username,'password' : password,'referer' : 'www.arcgis.com','f' : 'json' }

    r = requests.post(url, data=payload)

    token =json.loads(r.content)
    aToken = token['token']

    return aToken


def CreateUrl(base_url, params):
    base_url += '?'
    
    for k,v in params.items():
        base_url += '{}={}&'.format(k,v)
        
    base_url += 'f=json'
    
    print(base_url)
    return base_url
                
                

def ApiCall(url, data, token): #, serverGen):
    #extracts changes since specified serverGen and returns them as an object
    #url = service url
    #token = token as string

    url = CreateUrl(url, data)

    response = requests.post(url)

    print(response.content)

    url = json.loads(response.content)["statusUrl"]
    data  = {'token': token,
             'f': 'json'}

    url = CreateUrl(url, data)

    while True:
        time.sleep(3)
        response = requests.post(url)
        content = json.loads(response.content)
        if (content["status"] != 'Pending'):
            break

    
    if content['status'] == 'Failed':
        print(content['error'])
        return

    else:
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

    data  = {'token': token,
            'returnUpdates': True}

    url = CreateUrl(base_url, data)
    
    response = requests.post(url)

    if(response.status_code !=  200):
        print('HTTP Error code: {}'.format(response.status_code))
        return

    try: 
        content = json.loads(response.content)
    except:
        print('Error parsing response!')
        return

    try:
        serverGens = content["changeTrackingInfo"]['layerServerGens']
        capabilities = content['capabilities']
    except:
        print('Response missing servergens or capabilities!')
        return

    serverGen = [g for g in serverGens if g['id'] == layer]

    try:
        serverGen = serverGen[0]
    except:
        print('Layer {} does not exist'.format(layer))
        return

    capabilities = capabilities.lower()

    required = ['update', 'changetracking', 'create', 'delete', 'update', 'editing']

    capable = True

    for req in required:
        if not req in capabilities:
            print('Missing capability: {}'.format(req))
            capable = False

    return capable, serverGen

def ExtractChanges(url, layer, serverGen, token):
    #extracts changes since specified serverGen and returns them as an object

    data  = {'token': token,
            'layers': [layer],
            'returnInserts': 'true',
            'returnUpdates': 'true',
            'returnDeletes': 'true',
            'layerServerGens':  serverGens,
            'dataFormat': 'json'}

               
    url = base_url + '/extractChanges'
    
    response = ApiCall(url, data, token)

    return response['edits'][0]['features']

def ApplyEdits(url, layer, token, deltas):
    #applies edits to service, returns new serverGen/success code

    print(deltas)
    
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

token = GetToken(base_url, 'REDW_Python', 'Benefit4u!')
#print(CheckService(base_url, 0, token))
#serverGens = GetServerGen(base_url, token)
#serverGens = [{'id': 0, 'minServerGen': 54927109, 'serverGen': 56891349}]
#print(serverGens)
#deltas = ExtractChanges(base_url, 0, serverGens, token)
#deltas['updates'] = deltas['adds']
#deltas['adds'] = []
#print(json.dumps(deltas, indent=4))
print(ApplyEdits(base_url, 0, token, deltas))
