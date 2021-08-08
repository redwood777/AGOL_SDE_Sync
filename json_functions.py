import json
#import pandas as pd
import requests
#hello leo
#https://developers.arcgis.com/rest/
#https://developers.arcgis.com/rest/services-reference/enterprise/extract-changes-feature-service-.htm
#https://developers.arcgis.com/rest/services-reference/enterprise/apply-edits-feature-service-.htm

def GetURL(base_url, dictURL):
        base_url += '?'
        for k,v in dictURL.items():
                base_url += '{}={}&'.format(k,v)
        base_url += 'f=json'
        return base_url
        
def GetToken(base_url, username, password):
        #base_url: something like "https://nps.maps.arcgis.com"
        #uses AGOL rest API to aquire token with username and password
	url = 'https://nps.maps.arcgis.com/sharing/generateToken'
	payload  = {'username' : username,
				'password' : password,
				'referer' : 'www.arcgis.com',
				'f' : 'json'
                                }

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

def AsyncRequest(url, token):
        #send request to first url
        
        #get status url

        #wait for status url to stop being 'Pending'

        #if status = 'error'
        #throw error

        #otherwise get result url

        #send request to result url, return result
        

def ExtractChanges(base_url, token, serverGens):
    #extracts changes since specified serverGen and returns them as an object
        #url = service url
        #token = token as string
        base_url += '/extractChanges'
        dictURL = {'token' : token,
                   'layers' : '0',
                   'returnInserts' : True,
                   'returnUpdates' : True,
                   'returnDeletes' : True,
                   'layerServerGens' : serverGens
                   }
        url = GetURL(base_url, dictURL)

        data = requests.post(url)
	#print(url)
        print(data.content)
        content = json.loads(data.content)
        #returns pandas data fra
        
def ApplyEdits(url, token, deltas):
        #applies edits to service, returns new serverGen/success code
        #deltas = dictionary
        #convert to json and upload to AGOL
    return None

base_url = 'https://services1.arcgis.com/fBc8EJBxQRMcHlei/arcgis/rest/services/REDW_AGOL_PythonSyncTest_py/FeatureServer'


token = GetToken(base_url, 'REDW_Python', 'Benefit4u!')
serverGens = [{'id': 0, 'minServerGen': 54927109, 'serverGen': 56891349}]
ExtractChanges(base_url, token, serverGens)
url = 'https://nps.maps.arcgis.com/sharing/generateToken'
##payload  = {'username' : 'username',
##				'password' : 'password',
##				'referer' : 'www.arcgis.com'
##                                }
##
##print(GetURL(url,payload))

