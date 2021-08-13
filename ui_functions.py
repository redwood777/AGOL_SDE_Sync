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

def ResolveConflicts(SDE_deltas, AGOL_deltas):
    #Finds all conflicting edits. Resolves conflicts by user input. Returns revised SDE_deltas and AGOL_deltas

    print('Checking for conflicts...\n')
    #From here on, we will work only with global ids
    #SDE_added = GetGlobalIds(SDE_deltas['adds'])
    #AGOL_added = GetGlobalIds(AGOL_deltas['adds'])
    SDE_updated = GetGlobalIds(SDE_deltas['updates'])
    AGOL_updated = GetGlobalIds(AGOL_deltas['updates'])
    
    #remove deletes that have already occured in destination, and store as a set
    SDE_deleted = set(SDE_deltas['deleteIds']).difference(AGOL_deltas['deleteIds'])
    AGOL_deleted = set(AGOL_deltas['deleteIds']).difference(SDE_deltas['deleteIds'])
    
    #print("SDE_deleted:", SDE_deleted)
    #print("AGOL_deleted:", AGOL_deleted)
    #print("SDE_updated:", SDE_updated)
    #print("AGOL_updated:", AGOL_updated)


    #find update/delete conflictions
    AGOL_updated_SDE_deleted = AGOL_updated.intersection(SDE_deleted)
    SDE_updated_AGOL_deleted = SDE_updated.intersection(AGOL_deleted)

    #find update/update conflictions
    both_updated = AGOL_updated.intersection(SDE_updated)

    #print("AGOL_updated_SDE_deleted:", AGOL_updated_SDE_deleted)
    #print("SDE_updated_AGOL_deleted:", SDE_updated_AGOL_deleted)
    #print("both_updated:", both_updated)
    
    #print(json.dumps(AGOL_deltas, indent=4))
    #print(json.dumps(SDE_deltas, indent=4))

    #calculate sum of conflicts
    total_conflicts = len(AGOL_updated_SDE_deleted) + len(SDE_updated_AGOL_deleted) + len(both_updated)

    if(total_conflicts < 1):
        print('No conflicts found.\n')
    else:
        #display sum of conflicts
        #prompt user to resolve all one way, resolve manually, show more info, or cancel
        prompt = '{} conflicts found. Choose conflict resolution:'.format(total_conflicts)
        menu = ['Prioritize AGOL Changes', 'Prioritize SDE Changes', 'Choose for each conflict', 'More info', 'Cancel']
        choice = Options(prompt, menu)

        #in update/delete conflicts, update will either become add or be removed
        AGOL_updated -= AGOL_updated_SDE_deleted
        SDE_updated -= SDE_updated_AGOL_deleted
        
        #print(choice)
        #sets to store global ids of objects being moved from updates to adds
        AGOL_new_adds = set()
        SDE_new_adds = set()

        #if all in favor of AGOL:
        if (choice == 1):
            #move AGOL_updated_SDE_deleted from AGOL_updates to AGOL_adds
            AGOL_new_adds = AGOL_updated_SDE_deleted
            
            #remove AGOL_updated_SDE_deleted from SDE_deletes
            SDE_deleted -= AGOL_updated_SDE_deleted

            #remove both_updated from SDE updated
            SDE_updated -= both_updated

        #same for all in favor of SDE:
        if (choice == 2):
            SDE_new_adds = SDE_updated_AGOL_deleted         
            AGOL_deleted -= SDE_updated_AGOL_deleted
            AGOL_updated -= both_updated

        #if manual:
        if (choice == 3):
            #run through all conflict lists, print out conflict, prompt to resolve in favor of AGOL or SDE
            menu = ['Keep update from AGOL', 'Keep delete from SDE']
            #for update/delete conflicts:
            for conflict in AGOL_updated_SDE_deleted:
                prompt = 'Object "{}" was updated in AGOL and deleted in SDE. Choose:'.format(conflict)
                choice = Options(prompt, menu)
                
                #if in favor of update: update -> add, delete removed
                if (choice == 1):
                    AGOL_new_adds.add(conflict)
                    SDE_deleted.remove(conflict)
                #if in favor of delete: update removed (already done above)

            menu = ['Keep delete from AGOL', 'Keep update from SDE']
            
            for conflict in SDE_updated_AGOL_deleted:
                prompt = 'Object "{}" was deleted in AGOL and updated in SDE. Choose:'.format(conflict)
                choice = Options(prompt, menu)
                
                #if in favor of update: update -> add, delete removed
                if (choice == 2):
                    SDE_new_adds.add(conflict)
                    AGOL_deleted.remove(conflict)
                #if in favor of delete: update removed (already done above)
                

            #for update/update conflicts:
            menu = ['Keep update from AGOL', 'Keep update from SDE']

            for conflict in both_updated:
                prompt = 'Object "{}" was updated in both AGOL and SDE. Choose:'.format(conflict)
                choice = Options(prompt, menu)
                #losing update removed
                if(choice == 1):
                    SDE_updated.remove(conflict)
                elif(choice == 2):
                    AGOL_updated.remove(conflict)
                
        #build new json objects:

        #lists to store new updates
        revisedSdeUpdates = []
        revisedAgolUpdates = []

        #run through old updates and add them to new updates or adds
        for update in SDE_deltas['updates']:
            GUID = update['attributes']['GlobalID']
            if GUID in SDE_updated:
                revisedSdeUpdates.append(update)
            if GUID in SDE_new_adds:
                SDE_deltas['adds'].append(update)

        for update in AGOL_deltas['updates']:
            GUID = update['attributes']['GlobalID']
            if GUID in AGOL_updated:
                revisedAgolUpdates.append(update)
            if GUID in AGOL_new_adds:
                AGOL_deltas['adds'].append(update)

        #overwrite old updates
        AGOL_deltas['updates'] = revisedAgolUpdates
        SDE_deltas['updates'] = revisedSdeUpdates

    #overwrite old deletes (even if no conflicts, because deletes are checked for uniqueness above)
    AGOL_deltas['deleteIds'] = list(AGOL_deleted)
    SDE_deltas['deleteIds'] = list(SDE_deleted)

    #print("SDE_deleted:", SDE_deleted)
    #print("AGOL_deleted:", AGOL_deleted)
    #print("SDE_updated:", SDE_updated)
    #print("AGOL_updated:", AGOL_updated)
    #print("SDE_new_adds:", SDE_new_adds)
    #print("AGOL_new_adds:", AGOL_new_adds)
    
    #print(json.dumps(AGOL_deltas, indent=4))
    #print(json.dumps(SDE_deltas, indent=4))
    
       
    return SDE_deltas, AGOL_deltas
