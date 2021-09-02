import json
import requests
from ui_functions import Debug
import time

def GetToken(url, username, password):
    #returns token for use with further requests

    Debug('Getting AGOL token...', 2)
    
    url = url + '/sharing/generateToken'
    payload  = {'username' : username,'password' : password,'referer' : 'www.arcgis.com','f' : 'json' }

    r = requests.post(url, data=payload)

    response = json.loads(r.content)

    if not response.has_key('token'):
        print('No token returned!')
        print(response)
        return

    Debug('Token aquired.\n', 2, indent=4)
    
    return response['token']


def CreateUrl(base_url, params):
    base_url += '?'
    
    for k,v in params.items():
        base_url += '{}={}&'.format(k,v)
        
    base_url += 'f=json'
    
    Debug('URL:\n{}\n'.format(base_url), 3)
    
    return base_url
                
                
def ApiCall(url, data, token): #, serverGen):
    #performs async rest api call 

    Debug('Sending API request...', 2)

    url = CreateUrl(url, data)
    response = requests.post(url)

    url = json.loads(response.content)["statusUrl"]  
    data  = {'token': token}
    url = CreateUrl(url, data)

    while True:
        time.sleep(3)

        Debug('Checking status URL...', 2, indent=4)
        response = requests.post(url)
        content = json.loads(response.content)
        Debug('Status: {}'.format(content['status']), 2, indent=6)
        
        if (content["status"] != 'Pending'):
            break

    if content['status'] == 'Failed':
        print(content['error'])
        return

    else:
        Debug('Getting result...', 2, indent=4)
        
        url = content['resultUrl']
        url = CreateUrl(url, data)

        response = requests.post(url)
        content = json.loads(response.content)
        #print(json.dumps(content, indent=4))
        Debug('Done.\n', 2, indent=4)

    return content

def CheckService(base_url, layer, token): #, serverGen):
    #returns None if issue with service
    #returns False if service is missing capabilities
    #returns True, serverGen if service is set up correctly

    Debug('Checking AGOL service capabilities...', 1)

    data  = {'token': token,
            'returnUpdates': True}

    url = CreateUrl(base_url, data)
    
    response = requests.post(url)

    if(response.status_code !=  200):
        print('HTTP Error code: {}'.format(response.status_code))
        return False, None, None

    try: 
        content = json.loads(response.content)
        capabilities = content['capabilities']
    except:
        print('Error parsing response!')
        return False, None, None

    capabilities = capabilities.lower()
    required = ['update', 'changetracking', 'create', 'delete', 'update', 'editing']

    for req in required:
        if not req in capabilities:
            print('Missing capability: {}'.format(req))
            return False, None, None

    serverGens = content["changeTrackingInfo"]['layerServerGens']
    serverGen = [g for g in serverGens if g['id'] == layer]

    try:
        serverGen = serverGen[0]
    except:
        print('Layer {} does not exist'.format(layer))
        return False, None, None

    srid = content['spatialReference']['wkid']

    Debug('Feature service is valid.\n', 1, indent=4)
    
    return True, serverGen, srid

def ExtractChanges(url, layer, serverGen, token):
    #extracts changes since specified serverGen and returns them as an object

    Debug('Extracting changes from AGOL...\n', 1)

    data  = {'token': token,
            'layers': [layer],
            'returnInserts': 'true',
            'returnUpdates': 'true',
            'returnDeletes': 'true',
            'layerServerGens': json.dumps([serverGen]),
            'dataFormat': 'json'}

               
    url = url + '/extractChanges'
    
    response = ApiCall(url, data, token)

    try:
        deltas = response['edits'][0]['features']
    except:
        return False

    Debug('Success.\n', 1, indent=4)

    return deltas

def ApplyEdits(url, layer, token, deltas):
    #applies edits to service, returns success boolean

    Debug('Applying edits to AGOL...\n', 1)

    deltas['deletes'] = deltas.pop('deleteIds')
    
    deltas['id'] = layer

    #print(json.dumps(deltas, indent=4))

    data = {'token': token,
            'edits': json.dumps([deltas]),
            'useGlobalIds': 'true'}

    url += '/applyEdits?f=json'

    #url = CreateUrl(url, data)

    Debug('\n{}\n'.format(url), 3)

    response = requests.post(url, data=data) #, json={'edits': deltas})

    if(response.status_code != 200):
        print('HTTP Error code: {}\n'.format(response.status_code))
        return False

    print(response.content)

    try:
        content = json.loads(response.content)
    except:
        print('Invalid response')
        return False

    print(json.dumps(content, indent=4))

    try:
        error = content['error']
        print('Error: {}\n{}'.format(json.dumps(error, indent=4)))
        return False
    
    except:
        content = content[0]

        success = True

        for results in ['addResults', 'updateResults', 'deleteResults']:
            if (content.has_key(results)):
                for result in content[results]:
                    if not result['success']:
                        print(result['error'])
                        success = False

        if(not success):
            return False

        Debug('Success.\n', 1, indent=4)
        
        return True



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
