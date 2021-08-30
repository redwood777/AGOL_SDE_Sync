import json
#import pandas as pd
import requests
import ui_functions as ui
import time

def GetToken(base_url, username, password):
        #base_url: something like "https://nps.maps.arcgis.com"
        #uses AGOL rest API to aquire token with username and password
        url = 'https://nps.maps.arcgis.com/sharing/generateToken'
        payload  = {'username' : username,'password' : password,'referer' : 'www.arcgis.com','f' : 'json' }
        
        r = requests.post(url, data=payload)
        #print(r.content)
        
        token =json.loads(r.content)
        aToken = token['token']
        #print(aToken)
	
        return aToken

def CheckService(url, token):
        #url = service url
        #token = token as string
        #ensures that the service exists and has been set up correctly
        #returns true or false
    return True

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

            print(url)
            response = requests.post(url)
            content = json.loads(response.content)
            #print(json.dumps(content, indent=4))

    return content


def GetServerGen(base_url, token): #, serverGen):
    #extracts changes since specified serverGen and returns them as an object
    #url = service url
    #token = token as string
    data  = {'token': token,
            'returnUpdates': True,}

    #headers = {'Authentication': 'token {}'.format(token)}
               
    url = base_url

    url = CreateUrl(url, data)
    
    response = requests.post(url)
    #print(url)
    content = json.loads(response.content)
    #returns pandas data frame
    return content["changeTrackingInfo"]['layerServerGens']

def ExtractChanges(base_url, serverGens, token): #, serverGen):
    #extracts changes since specified serverGen and returns them as an object
        #url = service url
        #token = token as string
        data  = {'token': token,
                'layers': [0],
                'returnInserts': 'true',
                'returnUpdates': 'true',
                'returnDeletes': 'true',
                'layerServerGens':  serverGens,
                'dataFormat': 'json'}

        #headers = {'Authentication': 'token {}'.format(token)}
                   
        url = base_url + '/extractChanges'
        print(url)
	#print(url)
        response = ApiCall(url, data, token)    

        return response['edits'][0]['features']

#url = 'https://services1.arcgis.com/fBc8EJBxQRMcHlei/ArcGIS/rest/services/REDW_AGOL_PythonSyncTest_py/FeatureServer/jobs/74955bf0-ea4a-44a6-ad3c-0eae21a63642'

def ApplyEdits(url, token, deltas):
    #applies edits to service, returns new serverGen/success code

    deltas = [deltas.update({'id': 0})]

    data = {'token': token,
            'useGlobalIds': 'true'}

    url += '/applyEdits'

    url = CreateUrl(url, data)

    response = requests.post(url, json={'edits': deltas})
    print(response.content)
    return None

base_url = 'https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/REDW_AGOL_PythonSyncTest_py/FeatureServer'


token = GetToken(base_url, 'REDW_Python', 'Benefit4u!')
#serverGens = GetServerGen(base_url, token)
serverGens = [{'id': 0, 'minServerGen': 54927109, 'serverGen': 56891349}]
#print(serverGens)
deltas = ExtractChanges(base_url, serverGens, token)
ApplyEdits(base_url, token, deltas)
