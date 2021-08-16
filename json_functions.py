qimport json
import pandas as pd
import requests
#hello leo
#https://developers.arcgis.com/rest/
#https://developers.arcgis.com/rest/services-reference/enterprise/extract-changes-feature-service-.htm
#https://developers.arcgis.com/rest/services-reference/enterprise/apply-edits-feature-service-.htm
#https://pandas.pydata.org/docs/
def GetToken(base_url, username, password):
        #base_url: something like "https://nps.maps.arcgis.com"
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
	#returns token as a string
	return aToken

def CheckService(url, token):
        #url = service url
        #token = token as string
        #ensures that the service exists and has been set up correctly
        #returns true or false
    return True

def ExtractChanges(base_url, token): #, serverGen):
    #extracts changes since specified serverGen and returns them as an object
        #url = service url
        #token = token as string
	url = base_url + '/extractChanges?layers=0;returnInserts=true;returnUpdates=true;returnDeletes=true;layerServerGens=[{"id":0,"minServerGen":1529667,"serverGen":1534028}]};dataFormat=json;f=json;token=' + token
	data = requests.post(url)
	#print(url)
	print(data.content)
	#returns pandas data frame
	return None

def ApplyEdits(url, token, deltas):
        #applies edits to service, returns new serverGen/success code
        #deltas = pandas object
        #convert to json and upload to AGOL
    return None

base_url = 'https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/REDW_AGOL_PythonSyncTest_py/FeatureServer'


token = GetToken(base_url, 'REDW_Python', 'Benefit4u!')
ExtractChanges(base_url, token)

