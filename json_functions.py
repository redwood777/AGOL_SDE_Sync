import json
import pandas as pd
import requests
#hello leo
def GetToken(base_url, username, password):
    #uses AGOL rest API to aquire token with username and password
	url = 'https://nps.maps.arcgis.com/sharing/generateToken'
	payload  = {'username' : username,
				'password' : password,
				'referer' : 'www.arcgis.com',
				'f' : 'json' }

	r = requests.post(url, data=payload)

	token =json.loads (r.text)

	aToken = token['token']

	print(aToken)
	
	return aToken

def CheckService(url, token):
    #ensures that the service exists and has been set up correctly
    return True

def ExtractChanges(base_url, token): #, serverGen):
    #extracts changes since specified serverGen and returns them as an object
	url = base_url + '/extractChanges?layers=0;returnInserts=true;returnUpdates=true;returnDeletes=true;layerServerGens=[{"id":0,"minServerGen":1529667,"serverGen":1534028}]};dataFormat=json;f=json;token=' + token
	data = requests.post(url)
	#print(url)
	print(data.content)
	return None

def ApplyEdits(url, token, deltas):
    #applies edits to service, returns new serverGen/success code
    return None

base_url = 'https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/REDW_AGOL_PythonSyncTest_py/FeatureServer'


token = GetToken(base_url, 'REDW_Python', 'Benefit4u!')
ExtractChanges(base_url, token)

