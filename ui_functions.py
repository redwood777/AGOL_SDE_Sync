def GetGlobalIds(dict_in):
    #pulls global ids from adds or updates dictionary, returns as set
    return {add['attributes']['GlobalID'] for add in dict_in}

def Options(prompt, menu):
    i = 1
    print(prompt)
    for item in menu:
        print('  {}. {}'.format(i, item))
        i+=1
    return input('Enter selection:')

def ResolveConflicts(SECOND_deltas, FIRST_deltas, first_name, second_name):
    #Finds all conflicting edits. Resolves conflicts by user input. Returns revised SECOND_deltas and FIRST_deltas

    print('Checking for conflicts...\n')
    #From here on, we will work only with global ids
    #SECOND_added = GetGlobalIds(SECOND_deltas['adds'])
    #FIRST_added = GetGlobalIds(FIRST_deltas['adds'])
    SECOND_updated = GetGlobalIds(SECOND_deltas['updates'])
    FIRST_updated = GetGlobalIds(FIRST_deltas['updates'])
    
    #remove deletes that have already occured in destination, and store as a set
    SECOND_deleted = set(SECOND_deltas['deleteIds']).difference(FIRST_deltas['deleteIds'])
    FIRST_deleted = set(FIRST_deltas['deleteIds']).difference(SECOND_deltas['deleteIds'])
    
    #print("SECOND_deleted:", SECOND_deleted)
    #print("FIRST_deleted:", FIRST_deleted)
    #print("SECOND_updated:", SECOND_updated)
    #print("FIRST_updated:", FIRST_updated)

    #find update/delete conflictions
    FIRST_updated_SECOND_deleted = FIRST_updated.intersection(SECOND_deleted)
    SECOND_updated_FIRST_deleted = SECOND_updated.intersection(FIRST_deleted)

    #find update/update conflictions
    both_updated = FIRST_updated.intersection(SECOND_updated)

    #print("FIRST_updated_SECOND_deleted:", FIRST_updated_SECOND_deleted)
    #print("SECOND_updated_FIRST_deleted:", SECOND_updated_FIRST_deleted)
    #print("both_updated:", both_updated)
    
    #print(json.dumps(FIRST_deltas, indent=4))
    #print(json.dumps(SECOND_deltas, indent=4))

    #calculate sum of conflicts
    total_conflicts = len(FIRST_updated_SECOND_deleted) + len(SECOND_updated_FIRST_deleted) + len(both_updated)

    if(total_conflicts < 1):
        print('No conflicts found.\n')
    else:
        #display sum of conflicts
        #prompt user to resolve all one way, resolve manually, show more info, or cancel
        prompt = '{} conflicts found. Choose conflict resolution:'.format(total_conflicts)
        menu = ['Prioritize {} Changes'.format(first_name), 'Prioritize {} Changes'.format(second_name), 'Choose for each conflict', 'More info', 'Cancel']
        choice = Options(prompt, menu)

        #in update/delete conflicts, update will either become add or be removed
        FIRST_updated -= FIRST_updated_SECOND_deleted
        SECOND_updated -= SECOND_updated_FIRST_deleted
        
        #print(choice)
        #sets to store global ids of objects being moved from updates to adds
        FIRST_new_adds = set()
        SECOND_new_adds = set()

        #if all in favor of FIRST:
        if (choice == 1):
            #move FIRST_updated_SECOND_deleted from FIRST_updates to FIRST_adds
            FIRST_new_adds = FIRST_updated_SECOND_deleted
            
            #remove FIRST_updated_SECOND_deleted from SECOND_deletes
            SECOND_deleted -= FIRST_updated_SECOND_deleted

            #remove both_updated from SECOND updated
            SECOND_updated -= both_updated

        #same for all in favor of SECOND:
        if (choice == 2):
            SECOND_new_adds = SECOND_updated_FIRST_deleted         
            FIRST_deleted -= SECOND_updated_FIRST_deleted
            FIRST_updated -= both_updated

        #if manual:
        if (choice == 3):
            #run through all conflict lists, print out conflict, prompt to resolve in favor of FIRST or SECOND
            menu = ['Keep update from {}'.format(first_name), 'Keep delete from {}'.format(second_name)]
            #for update/delete conflicts:
            for conflict in FIRST_updated_SECOND_deleted:
                prompt = 'Object "{}" was updated in {} and deleted in {}. Choose:'.format(conflict, first_name, second_name)
                choice = Options(prompt, menu)
                
                #if in favor of update: update -> add, delete removed
                if (choice == 1):
                    FIRST_new_adds.add(conflict)
                    SECOND_deleted.remove(conflict)
                #if in favor of delete: update removed (already done above)

            menu = ['Keep delete from {}'.format(first_name), 'Keep update from {}'.format(second_name)]
            
            for conflict in SECOND_updated_FIRST_deleted:
                prompt = 'Object "{}" was deleted in {} and updated in {}. Choose:'.format(conflict, first_name, second_name)
                choice = Options(prompt, menu)
                
                #if in favor of update: update -> add, delete removed
                if (choice == 2):
                    SECOND_new_adds.add(conflict)
                    FIRST_deleted.remove(conflict)
                #if in favor of delete: update removed (already done above)
                

            #for update/update conflicts:
            menu = ['Keep update from {}'.format(first_name), 'Keep update from {}'.format(second_name)]

            for conflict in both_updated:
                prompt = 'Object "{}" was updated in both {} and {}. Choose:'.format(conflict, first_name, second_name)
                choice = Options(prompt, menu)
                #losing update removed
                if(choice == 1):
                    SECOND_updated.remove(conflict)
                elif(choice == 2):
                    FIRST_updated.remove(conflict)
                
        #build new json objects:

        #lists to store new updates
        revisedSECONDUpdates = []
        revisedFIRSTUpdates = []

        #run through old updates and add them to new updates or adds
        for update in SECOND_deltas['updates']:
            GUID = update['attributes']['GlobalID']
            if GUID in SECOND_updated:
                revisedSECONDUpdates.append(update)
            if GUID in SECOND_new_adds:
                SECOND_deltas['adds'].append(update)

        for update in FIRST_deltas['updates']:
            GUID = update['attributes']['GlobalID']
            if GUID in FIRST_updated:
                revisedFIRSTUpdates.append(update)
            if GUID in FIRST_new_adds:
                FIRST_deltas['adds'].append(update)

        #overwrite old updates
        FIRST_deltas['updates'] = revisedFIRSTUpdates
        SECOND_deltas['updates'] = revisedSECONDUpdates

    #overwrite old deletes (even if no conflicts, because deletes are checked for uniqueness above)
    FIRST_deltas['deleteIds'] = list(FIRST_deleted)
    SECOND_deltas['deleteIds'] = list(SECOND_deleted)

    #print("SECOND_deleted:", SECOND_deleted)
    #print("FIRST_deleted:", FIRST_deleted)
    #print("SECOND_updated:", SECOND_updated)
    #print("FIRST_updated:", FIRST_updated)
    #print("SECOND_new_adds:", SECOND_new_adds)
    #print("FIRST_new_adds:", FIRST_new_adds)
    
    #print(json.dumps(FIRST_deltas, indent=4))
    #print(json.dumps(SECOND_deltas, indent=4))
    
       
    return SECOND_deltas, FIRST_deltas
