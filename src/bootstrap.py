from flask import Flask, render_template, request
import requests
import hashlib
import json
import sys 
import threading


replication_fact = int(sys.argv[2]) # k 
port = sys.argv[1] 
method = sys.argv[3] #either lin or even 

info = {'replication_factor' : replication_fact, 'port' : port, 'method' : method}

app = Flask(__name__)
nodes = [] #list with all the ids of the nodes joined the chord
node_addr = {} #save the ip of a node given the his id as the key
bootstrap_addr = {} #address of bootstrap and his successor and predecessor
files = {} #save the files in this format files[hashed_key] = [key, value, replication_factor]
#if we dont have replicas the replication factor is 1 constantly

def thread_write_data(request_string, request_json):
    requests.post(request_string, json = request_json)
    return

#we will probably not use locks because 2 threads
#will not write the same file at the same time
def evcon(request_string, request_json):
    thread1 = threading.Thread(target=thread_write_data,args=(request_string, request_json))
    thread1.start()
    return


def bootstrap_join(): #first function to run // bootstrap joins first
    #hashing
    hash_value = hashlib.sha1()
    hash_value.update('192.168.0.1'.encode())
    hash_value.update('5000'.encode())
    tmp = hash_value.hexdigest()
    node_id = int(tmp, 16)
    nodes.append(node_id) #insert to list
    #save values of bootstrap
    bootstrap_addr['node_ID'] = nodes[0]
    bootstrap_addr['successor_ID'] = nodes[0]
    bootstrap_addr['predecessor_ID'] = nodes[0]
    node_addr[node_id] = "192.168.0.1:5000"
    bootstrap_addr['successor_IP'] = node_addr[node_id]
    bootstrap_addr['predecessor_IP'] = node_addr[node_id]
    print('First Node initialized (Bootstrap)')
    print('Bootstrap has an ID of : ', node_id)
    print('Total nodes of the system until now : ', nodes)
    print('')

def find_successor(id):#sorts nodes based on id and returns successor value
    if len(nodes) == 2:
        if id != nodes[0]:
            return nodes[0]
        else:
            return nodes[1]
    nodes.sort()
    if nodes.index(id) == len(nodes) - 1:
        return nodes[0]
    else:
        return nodes[nodes.index(id) + 1]

def get_key(val, dict):#look for a value in a dictionary and return key if found
    for key, value in dict.items():
        if val == value:
            return key
    return "Couldnt find the key"

def find_predecessor(id):#sorts nodes based on id and returns predeccesor value
    if len(nodes) == 2:
        if id != nodes[0]:
            return nodes[0]
        else:
            return nodes[1]
    nodes.sort() 
    if nodes.index(id) == 0:
        return nodes[len(nodes) - 1]
    else:
        return nodes[nodes.index(id) - 1]

def Keepthekey(key):
    if bootstrap_addr['node_ID'] < bootstrap_addr['predecessor_ID']: #case of a loop
        return (int(key) > bootstrap_addr['predecessor_ID'] or int(key) <= bootstrap_addr['node_ID'])
    else: #bootstrap is in between
        return (int(key) <= bootstrap_addr['node_ID'] and int(key) > bootstrap_addr['predecessor_ID'])


@app.route('/join', methods = ['POST', 'GET'])
def join():
 
    if request.method == 'POST':
        #hashing
        address = json.loads(request.json)
        hash_value = hashlib.sha1()
        hash_value.update(address['ip'].encode())
        hash_value.update(address['port'].encode())
        tmp = hash_value.hexdigest()
        #https://stackoverflow.com/questions/209513/convert-hex-string-to-int-in-python
        node_id = int(tmp, 16)
        nodes.append(node_id)
        print('')
        print('Total nodes so far : ', nodes)
        node_addr[node_id] = str(address['ip'] + ':' + address['port'])
        print('Node with address : ', node_addr[node_id], ' just arrived')
        print('The ID of the new node is: ', node_id)
    if len(nodes) > 1:   #you have to update successor and predecessor
        successor = find_successor(node_id) 
        predecessor = find_predecessor(node_id) 
        print('Successor of node entered : ', successor)
        print('Predecessor of node entered : ', predecessor)
        print('')
        requests.post(str('http://'+address['ip']+':'+address['port']+'/initialize'), json = json.dumps({'node_ID' : node_id, 'successor_ID' : successor, 
                                'successor_IP' : node_addr[successor], 'predecessor_ID' : predecessor, 'predecessor_IP' : node_addr[predecessor]}))
        requests.post(str('http://'+node_addr[successor]+'/update'), json = json.dumps({'predecessor_ID' : node_id, 'predecessor_IP' : node_addr[node_id]}))
        requests.post(str('http://'+node_addr[predecessor]+'/update'), json = json.dumps({'successor_ID' : node_id, 'successor_IP' : node_addr[node_id]}))
        requests.post(str('http://' + node_addr[successor] + '/update_files_join'), json = json.dumps({'ip' : address['ip'], 'port' : address['port']}))

    return "Hi"

@app.route('/update', methods = ['POST', 'GET'])  #update the successor and predecessor of bootstrap node
def update():
    if request.method == 'POST':
        result = json.loads(request.json)
        if 'successor_ID' in result:
            bootstrap_addr['successor_ID'] = result['successor_ID']
            bootstrap_addr['successor_IP'] = result['successor_IP']
        elif 'predecessor_ID' in result:
            bootstrap_addr['predecessor_ID'] = result['predecessor_ID']
            bootstrap_addr['predecessor_IP'] = result['predecessor_IP']
        print('------------------------- Update for bootstrap node -------------------------')
        print('Successor : ',bootstrap_addr['successor_ID'], ' and Predecessor : ', bootstrap_addr['predecessor_ID'])
        print('')
    return "Hi"


@app.route('/update_files_join', methods = ['POST', 'GET']) #this runs by the successor
def update_files_join():
    replication_factor = info['replication_factor']
    if request.method == 'POST':
        not_here = {}
        saved = []
        data = json.loads(request.json)
        address = str(data['ip'] + ':' + data['port']) #address of the joined node
        if replication_factor == 1:#we dont have replicas 
            for k,v in files.items():
                key,val,_ = v
                if not Keepthekey(k): #find which files need to be moved to the new node.
                    not_here[k] = [key,val, replication_factor] #not here contains all the keys-values that need to be moved.
                    saved.append(k)
            for item in saved: #delete files tha need to be moved from the successor of the joined node
                del files[item]
            
            if not not_here:
                print('Node with IP : ',address, 'isnt responsible for any files')
            #maybe start replicas here 1
            else:
                print('Deleting and sending to node with address: ',address, 'the files that it is responsible for')
                requests.post(str('http://' + address + '/updated_files_join'), json=json.dumps(not_here))

        elif info['method'] == 'lin' or info['method'] == 'even': #if we have replicas
            for key in files.copy(): 
                if not Keepthekey(key):
                    if (int(files[key][2]) > 1):
                        requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/delete_replicas'), json = json.dumps({'replication_factor' : int(files[key][2]) - 1, 'key' : files[key][0], 'value' : files[key][1]}))
                        requests.post(str('http://' + bootstrap_addr['predecessor_IP'] + '/insert_replicas'), json = json.dumps({'replication_factor' : int(files[key][2]), 'key' : files[key][0], 'value' : files[key][1]}))
                    else:
                        requests.post(str('http://' + bootstrap_addr['predecessor_IP'] + '/insert_replicas'), json = json.dumps({'replication_factor' : int(files[key][2]), 'key' : files[key][0], 'value' : files[key][1]}))
                        del files[key]
    return "Hi"

@app.route('/updated_files_join', methods = ['POST', 'GET']) #this is run by the joining node (only for none as consistency type)
def updated_files_join(): #tranfer files to new responsible node
    replication_factor = info['replication_factor']
    if request.method == 'POST':
        data = json.loads(request.json)
        for k,v in data.items():
            key,val, _ = v
            files[k] = [key,val, replication_factor]
        print('This node is responsible for the files shown below')
        print("{:<15} {:<20} {:<7}".format('Hashed_Key','Key','Value'))
        for k,v in files.items():
            key, val,_ = v
            print("{:<15} {:<20} {:<7}".format(k, key, val))
        print('')

    return "Hi"

    
@app.route('/depart', methods = ['POST', 'GET'])  
def depart():
    replication_factor = info['replication_factor']                                   
    if request.method == 'POST':
        data = json.loads(request.json)  # node ip
        result = data[0] #result has ip and port
        datas = data[1] #datas = files
        print(result)
        addr = str(result['ip'] + ':' + result['port'])
        node_id = get_key(addr, node_addr)
        successor = find_successor(node_id)
        predecessor = find_predecessor(node_id)
        print('Node with an ID of: ', node_id, 'needs to depart...')
        print('Need to update nodes : ', successor, 'and', predecessor)
        if replication_factor == 1: #if we dont have replicas
            requests.post(str('http://'+node_addr[successor]+'/update_depart'), json = json.dumps({'predecessor_ID' : predecessor, 'predecessor_IP' : node_addr[predecessor]}))
            requests.post(str('http://'+node_addr[predecessor]+'/update_depart'), json = json.dumps({'successor_ID' : successor, 'successor_IP' : node_addr[successor]}))
            requests.post(str('http://' + node_addr[successor] + '/update_files_depart'), json = json.dumps(datas))
            print('...')
            print('Node ', node_id, ' removed')
            requests.post(str('http://'+node_addr[node_id]+'/terminate'))
            nodes.remove(node_id)
            del node_addr[node_id]
            print('Nodes in the chord are : ', nodes,'\n')
        elif info['method'] == 'lin' or info['method'] == 'even': #if we have replicas (lin or even)
            requests.post(str('http://'+node_addr[successor]+'/update_depart'), json = json.dumps({'predecessor_ID' : predecessor, 'predecessor_IP' : node_addr[predecessor]}))
            requests.post(str('http://'+node_addr[predecessor]+'/update_depart'), json = json.dumps({'successor_ID' : successor, 'successor_IP' : node_addr[successor]}))
            requests.post(str('http://' + node_addr[node_id] + '/update_files_depart_replicas'), json = json.dumps({'data':datas, 'replication_factor':replication_factor}))
            print('...')
            print('Node ', node_id, ' removed')
            requests.post(str('http://'+node_addr[node_id]+'/terminate'))
            nodes.remove(node_id)
            del node_addr[node_id]
            print('Nodes in the chord are : ', nodes,'\n')
            

    return "Hi"

@app.route('/update_depart', methods = ['POST', 'GET'])  #update bootstrap's successor and predecessor due to depart of a node
def update_depart():
    if request.method == 'POST':
        result = json.loads(request.json)
        if 'successor_ID' in result:
            bootstrap_addr['successor_ID'] = result['successor_ID']
            bootstrap_addr['successor_IP'] = result['successor_IP']
        elif 'predecessor_ID' in result:
            bootstrap_addr['predecessor_ID'] = result['predecessor_ID']
            bootstrap_addr['predecessor_IP'] = result['predecessor_IP']
        print('------------------------- Update for bootstrap node -------------------------')
        print('Due to a node departing the chord bootstrap is affected...')
        print('Successor : ',bootstrap_addr['successor_ID'], ' and Predecessor : ', bootstrap_addr['predecessor_ID'])
        print('')
    return "Hi"
 

@app.route('/update_files_depart_replicas', methods = ['POST', 'GET'])  #update replicas due to departure of a node
def update_files_depart_replicas():
    if request.method == 'POST':
        for key in files.copy():
            requests.post(str('http://' + bootstrap_addr['successor_IP']+'/insert_replicas'), json = json.dumps({'replication_factor':int(files[key][2]),'key':files[key][0], 'value':files[key][1]}))
    return "Hi"

@app.route('/update_files_depart', methods = ['POST', 'GET']) #called if we dont have replicas
def update_files_depart():
    if request.method == 'POST':

        data = json.loads(request.json)
        for k,v in data.items():
            key,val,_ = v
            if k not in files:
                files[k] = [key,val,info['replication_factor']]
        print('Due to a node depart the files are updated')
        print('The new files to this node are listed below:')
        print("{:<15} {:<20} {:<7}".format('Hashed_Key','Key','Value'))
        for k,v in files.items():
            key, val,_ = v
            print("{:<15} {:<20} {:<7}".format(k, key, val))
        print('')

    return "Hi"

@app.route('/query', methods = ['POST', 'GET'])
def query():
    replication_factor = info['replication_factor']
    if request.method == 'POST':
        data = json.loads(request.json) 
        hash_value = hashlib.sha1()
        hash_value.update(data['key'].encode())
        tmp = hash_value.hexdigest()
        hashed_key = int(tmp, 16)
        if replication_factor == 1: #if we dont have replicas
            if Keepthekey(hashed_key):
                print('I should have the key you asked for...')
                if hashed_key in files:
                    print('I found the value corresponding to', data['key'])
                    print('Value : ', files[hashed_key][1])
                else:
                    print('Someone messed up i cant find the key you asked for...')
            else:
                print('I am not responsible for the key you asked')
                print('Forwarding the request...')
                requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/query'), json= json.dumps({'key':data['key']}))
        else: #we have replicas
            if info['method'] == 'even':
                if hashed_key in files:
                    print('I found the value corresponding to', data['key'])
                    print('Value : ', files[hashed_key][1])
                    return (str(data['key']+':'+files[hashed_key][1]))
                else:
                    if Keepthekey(hashed_key):
                        print("Key wasnt found in Chord")
                        return "Hi"
                    else:
                        return(requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/query'), json= json.dumps({'key':data['key']})).text)  #search until you find a node that has the key you need
            elif info['method'] == 'lin':
                if Keepthekey(hashed_key):
                    requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/query_replica'), json= json.dumps({'key':data['key'] 
                                    , 'replication_factor' : replication_factor - 1}))
                    if hashed_key in files:
                        return(str(data['key']+':'+files[hashed_key][1]))
                    else:
                        return "Hi"
                else:
                    print('I am not responsible for the key you asked')
                    print('Forwarding the request...')
                    return(requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/query'), json= json.dumps({'key':data['key']})).text)
                    

    return "Hi"

@app.route('/query_replica', methods = ['POST', 'GET'])
def query_replica():
    if request.method == 'POST':
        data = json.loads(request.json)
        if data['replication_factor'] == 1:
            hash_value = hashlib.sha1()
            hash_value.update(data['key'].encode())
            tmp = hash_value.hexdigest()
            hashed_key = int(tmp, 16)
            if hashed_key in files:
                print('I found the value corresponding to', data['key'])
                print('Value : ', files[hashed_key][1])
            else: 
                print('Someone messed up i cant find the key you asked for...')
        else:
            if info['method'] == 'lin':
                requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/query_replica'), json= json.dumps({'key':data['key']
                                    , 'replication_factor' : data['replication_factor'] - 1})) #pass the request to k-1 successor
 
    return "Hi"

@app.route('/query_star', methods = ['POST', 'GET']) #if key is *
def query_star():
    if request.method == 'POST':
        data = json.loads(request.json)
        if data['myIP'] == '192.168.0.1:5000' and data['rep'] != 0:
            return "Hi"
        else:
            print('Printing all the files i have due to query key...\n')
            print("{:<20} {:<7}".format('Key','Value'))
            for v in files.values():
                key, val, _ = v #_ runs only for linearizability
                print("{:<20} {:<7}".format(key, val))
            print('')
            requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/query_star'), json = json.dumps({'myIP' : data['myIP'], 'rep' : 1}))
    return "Hi"



@app.route('/overlay', methods = ['POST', 'GET'])
def overlay():
    if request.method == 'POST':
        print('')
        print('Printing the overlay of the chord')
        print(nodes,'\n')
    return "Hi"

@app.route('/insert', methods = ['POST', 'GET'])#insert [key, value] to the chosen node
def insert():
    replication_factor = info['replication_factor']
    if request.method == 'POST':
        data = json.loads(request.json) 
        print('Key given is : ', data['key'])
        print('Value for that key is : ', data['value'])
        hash_value = hashlib.sha1()
        hash_value.update(data['key'].encode())
        tmp = hash_value.hexdigest()
        hashed_key = int(tmp, 16)

        if replication_factor == 1: #if we dont have replicas
            if Keepthekey(hashed_key):
                if hashed_key not in files:
                    files[hashed_key] = [data['key'], data['value'],replication_factor]
                print('Files in this node : \n')
                print("{:<15} {:<20} {:<7}".format('Hashed_Key','Key','Value'))
                for k,v in files.items():
                    key, val,_ = v
                    print("{:<15} {:<20} {:<7}".format(k, key, val))
                print('')
            else:
                print('Key doesnt belong to this node')
                print('Forwarding the key value pair to the successor...\n')
                requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/insert'), json = json.dumps({'key' : data['key'], 'value' : data['value']}))
        else: #if we have replicas
            if Keepthekey(hashed_key):
                data['replication_factor'] = replication_factor
                files[hashed_key] = [data['key'], data['value'], data['replication_factor']]
                if info['method'] == 'lin':
                    requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/insert_replicas'), json = json.dumps({'key' : data['key'], 
                    'value' : data['value'], 'replication_factor' : data['replication_factor'] - 1}))
                elif info['method'] == 'even': #code for eventual
                    evcon(str('http://' + bootstrap_addr['successor_IP'] + '/insert_replicas'), json.dumps({'key' : data['key'],  #send the thread to do the request
                    'value' : data['value'], 'replication_factor' : data['replication_factor'] - 1}))
                    return "Hi"
                
            else:
                print('Key doesnt belong to this node')
                print('Forwarding the key value pair to the successor...\n')
                requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/insert'), json = json.dumps({'key' : data['key'], 'value' : data['value']}))
    return "Hi"

@app.route('/insert_replicas', methods = ['POST', 'GET']) #insert the [key,value] replicas to the k-1 successors
def insert_replicas():
    if request.method == 'POST':
        data = json.loads(request.json)
        replication_factor = data['replication_factor']
        if replication_factor != 0: #continue as long as replication factor is greater than zero
            print('Key_replica given is : ', data['key'])
            print('Value_replica given is : ', data['value'])
            hash_value = hashlib.sha1()
            hash_value.update(data['key'].encode())
            tmp = hash_value.hexdigest()
            hashed_key = int(tmp, 16) 
            files[hashed_key] = [data['key'], data['value'], replication_factor]
            if info['method'] == 'lin':
                requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/insert_replicas'), json = json.dumps({'key' : data['key'], 
                'value' : data['value'], 'replication_factor' : replication_factor - 1}))
            elif info['method'] == 'even':
                evcon(str('http://' + bootstrap_addr['successor_IP'] + '/insert_replicas'), json.dumps({'key' : data['key'], #send the thread to do the request
                'value' : data['value'], 'replication_factor' : replication_factor - 1}))  
                return "Hi"
        else:
            return "Hi"
    return "Hi"


@app.route('/delete', methods = ['POST', 'GET']) #delete the [key,value] we want from all the nodes that contains it
def delete():                                    #including replicas in case we have
    replication_factor = info['replication_factor']
    if request.method == 'POST':
        data = json.loads(request.json)
        hash_value = hashlib.sha1()
        hash_value.update(data['key'].encode())
        tmp = hash_value.hexdigest()
        hashed_key = int(tmp, 16)

        if replication_factor == 1: #we dont have replicas
            if Keepthekey(hashed_key):
                if (data['key'] not in files[hashed_key]):
                    print('The key doesnt exist in this node...')
                    print('You propably messed up!')
                    return "Hi"
                print('Deleting the file with a hashed key of : ', hashed_key)
                del files[hashed_key]
                if files:
                    print('Files are presented below:')
                    print("{:<15} {:<20} {:<7}".format('Hashed_Key','Key','Value'))
                    for k,v in files.items():
                        key, val,_ = v
                        print("{:<15} {:<20} {:<7}".format(k, key, val))
                    print('')
                else:
                    print('There are no files in this node')
            else: 
                print('Key doesnt belong to this node')
                print('Forwarding the key to the successor...\n')
                requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/delete'), json = json.dumps({'key' : data['key']}))
        else: #we have replicas
            if Keepthekey(hashed_key):
                if (data['key'] not in files[hashed_key]):
                    print('The key doesnt exist in this node...')
                    print('You propably messed up!')
                    return "Hi"
                print('Deleting the file with a hashed key of : ', hashed_key)
                del files[hashed_key]
                if files:
                    mpravo = 1
                else:
                    print('There are no files in this node')
                if info['method'] == 'lin':
                    requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/delete_replicas'), json = json.dumps({'key' : data['key'],
                                                'replication_factor' : replication_factor - 1}))
                elif info['method'] == 'even':
                    evcon(str('http://' + bootstrap_addr['successor_IP'] + '/delete_replicas'), json.dumps({'key' : data['key'],
                                                'replication_factor' : replication_factor - 1}))
            else: 
                print('Key doesnt belong to this node')
                print('Forwarding the key to the successor...\n')
                requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/delete'), json = json.dumps({'key' : data['key']}))

    return "Hi"

@app.route('/delete_replicas', methods = ['POST', 'GET']) #find the nodes that contains the pair [key,value] and then it deletes it
def delete_replicas():
    if request.method == 'POST':
        data = json.loads(request.json)
        replication_factor = data['replication_factor']
        if replication_factor != 0:
            print('Key_replica given is : ', data['key'])
            hash_value = hashlib.sha1()
            hash_value.update(data['key'].encode())
            tmp = hash_value.hexdigest()
            hashed_key = int(tmp, 16)
            del files[hashed_key]
            if 'value' in data:
                if info['method'] == 'lin':
                    requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/delete_replicas'), json = json.dumps({'key' : data['key'], 
            'value' : data['value'], 'replication_factor' : replication_factor - 1}))
                elif info['method'] == 'even':
                    evcon(str('http://' + bootstrap_addr['successor_IP'] + '/delete_replicas'), json.dumps({'key' : data['key'], 
            'value' : data['value'], 'replication_factor' : replication_factor - 1}))
            else:
                if info['method'] == 'lin':
                    requests.post(str('http://' + bootstrap_addr['successor_IP'] + '/delete_replicas'), json = json.dumps({'key' : data['key'], 'replication_factor' : replication_factor - 1}))
                elif info['method'] == 'even':
                    evcon(str('http://' + bootstrap_addr['successor_IP'] + '/delete_replicas'), json.dumps({'key' : data['key'], 'replication_factor' : replication_factor - 1}))
            return "Hi"
    return "Hi"


bootstrap_join()


if __name__ == "__main__":
    app.run(host = "192.168.0.1", port=5000)  #everyone knows my IP
