import json

def GetToken(url, username, password):
    #uses AGOL rest API to aquire token with username and password
    token = None
    return token

def CheckService(url, token):
    #ensures that the service exists and has been set up correctly
    return True

def ExtractChanges(url, token, serverGen):
    #extracts changes since specified serverGen and returns them as an object
    return None

def ApplyEdits(url, token, deltas):
    #applies edits to service, returns new serverGen/success code
    return None
