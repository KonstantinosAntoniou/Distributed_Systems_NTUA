from flask import Flask, render_template, request
import requests
import hashlib
import json
import threading
import time
import socket
import sys
import threading, queue

replication_fact = int(sys.argv[2]) # k 
port = sys.argv[1]
method = sys.argv[3] #either lin or even 


info = {'replication_factor' : replication_fact, 'port' : port, 'method' : method}

if(socket.getfqdn()=='snf-18622'): myIp='192.168.0.1'
elif(socket.getfqdn()=='snf-18601'): myIp='192.168.0.2'
elif(socket.getfqdn()=='snf-18608'): myIp='192.168.0.3'
elif(socket.getfqdn()=='snf-18609'): myIp='192.168.0.4'
elif(socket.getfqdn()=='snf-18610'): myIp='192.168.0.5'

Ip = myIp
Port = info['port']
myIp = Ip + ':' + Port

app = Flask(__name__)
flag = 0
global addr
addr = {} #for each node we save its successor, predecessor and his ip
files = {} #save the files in this format files[hashed_key] = [key, value, replication_factor]
#if we dont have replicas the replication factor is 1 constantly 



def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

def Keepthekey(key):
    global addr
    if addr['node_ID'] < addr['predecessor_ID']: #case of a loop
        return (int(key) > addr['predecessor_ID'] or int(key) < addr['node_ID'])
    else: #bootstrap is in between
        return (int(key) < addr['node_ID'] and int(key) > addr['predecessor_ID'])

def thread_write_data(request_string, request_json):
    requests.post(request_string, json = request_json)
    return

#we will probably not use locks because 2 threads
#will not write the same file at the same time
def evcon(request_string, request_json):
    thread1 = threading.Thread(target=thread_write_data,args=(request_string, request_json))
    thread1.start()
    return


@app.route('/initialize', methods = ['POST', 'GET'])
def initialize():
    if request.method == 'POST':
        global addr
        addr = json.loads(request.json)
        print(addr)
        node_id = addr["node_ID"]
        print('I joined the chord')
        print('With an ID of : ', node_id)
        print('My succesor is : ', addr['successor_ID'], 'with an IP of : ', addr['successor_IP'])
        print('My predecessor is : ', addr['predecessor_ID'], 'with an IP of : ', addr['predecessor_IP'])
        print('')
        
    return "Hi"

@app.route('/update_files_join', methods = ['POST', 'GET']) #this is run by the joining node (only for none as consistency type)
def update_files_join(): #tranfer files to new responsible node
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
            else:
                print('Deleting and sending to node with address: ',address, 'the files that it is responsible for')
                requests.post(str('http://' + address + '/updated_files_join'), json=json.dumps(not_here))

        else:
            for key in files.copy():
                if not Keepthekey(key):
                    if (int(files[key][2]) > 1):
                        requests.post(str('http://' + addr['successor_IP'] + '/delete_replicas'), json = json.dumps({'replication_factor' : int(files[key][2]) - 1, 'key' : files[key][0], 'value' : files[key][1]}))
                        requests.post(str('http://' + addr['predecessor_IP'] + '/insert_replicas'), json = json.dumps({'replication_factor' : int(files[key][2]), 'key' : files[key][0], 'value' : files[key][1]}))
                    else:
                        requests.post(str('http://' + addr['predecessor_IP'] + '/insert_replicas'), json = json.dumps({'replication_factor' : int(files[key][2]), 'key' : files[key][0], 'value' : files[key][1]}))
                        del files[key]
    return "Hi"

@app.route('/update', methods = ['POST', 'GET']) #update the successor and predecessor of node
def update():
    if request.method == 'POST':
        global addr
        info = json.loads(request.json)
        if 'predecessor_ID' in info:
            addr['predecessor_ID'] = info['predecessor_ID']
            addr['predecessor_IP'] = info['predecessor_IP']
            
        elif 'successor_ID' in info:
            addr['successor_ID'] = info['successor_ID']
            addr['successor_IP'] = info['successor_IP']
        
        print('')
        print('----------- Update for node ',addr['node_ID'], '-----------')
        print('Successor : ', addr['successor_ID'])
        print('Predecessor : ', addr['predecessor_ID'])
    
    return "Hi"

@app.route('/updated_files_join', methods = ['POST', 'GET']) #this is run by the joining node (only for none as consistency type)
def updated_files_join(): #tranfer files to new responsible node
    if request.method == 'POST':
        data = json.loads(request.json)
        for k,v in data.items():
            key,val,_ = v
            files[k] = [key,val,info['replication_factor']]
        print('This node is responsible for the files shown below')
        print("{:<15} {:<20} {:<7}".format('Hashed_Key','Key','Value'))
        for k,v in files.items():
            key, val,_ = v
            print("{:<15} {:<20} {:<7}".format(k, key, val))
        print('')
        return "Hi" 


@app.route('/depart', methods = ['POST', 'GET']) #send request to bootstrap in order to depart
def depart():
    print('Sending request to bootstrap in order to depart from chord')
    boot_addr = str('http://192.168.0.1:5000/depart')
    requests.post(boot_addr, json=json.dumps([{'ip' : Ip, 'port' : Port}, files]))
    return "Hi"

@app.route('/update_depart', methods = ['POST', 'GET'])
def update_depart(): #update node's successor and predecessor due to depart of a node
    if request.method == 'POST':
        global addr
        info = json.loads(request.json)
        if 'predecessor_ID' in info:
            addr['predecessor_ID'] = info['predecessor_ID']
            addr['predecessor_IP'] = info['predecessor_IP']
            
        elif 'successor_ID' in info:
            addr['successor_ID'] = info['successor_ID']
            addr['successor_IP'] = info['successor_IP']
        
        print('')
        print('----------- Update for node ',addr['node_ID'], '-----------')
        print('Successor : ', addr['successor_ID'])
        print('Predecessor : ', addr['predecessor_ID'])
    
    return "Hi"

@app.route('/update_files_depart', methods = ['POST', 'GET'])
def update_files_depart(): ##update replicas due to departure of a node
    if request.method == 'POST':

        data = json.loads(request.json)
        for k,v in data.items():
            key,val,_ = v
            if k not in files:
                files[k] = [key,val,info['replication_factor']]
            
        print('Due to a node depart the files are updated')
        print('The new filed to this node are listed below:')
        print("{:<15} {:<20} {:<7}".format('Hashed_Key','Key','Value'))
        for k,v in files.items():
            key, val,_ = v
            print("{:<15} {:<20} {:<7}".format(k, key, val))
        print('')

    return "Hi" 

@app.route('/update_files_depart_replicas', methods = ['POST', 'GET'])
def update_files_depart_replicas():
    if request.method == 'POST':
        for key in files.copy():
            requests.post(str('http://' + addr['successor_IP']+'/insert_replicas'), json = json.dumps({'replication_factor':int(files[key][2]),'key':files[key][0], 'value':files[key][1]}))
    return "Hi"



@app.route('/terminate', methods = ['POST', 'GET']) #the node need to depart so we terminate
def terminate():
    print('Terminating....')
    shutdown_server()
    return 'Server Shutting down...\n Node departs from the chord...'

@app.route('/query', methods = ['POST', 'GET'])
def query():
    replication_factor = info['replication_factor']
    if request.method == 'POST':
        global addr
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
                requests.post(str('http://' + addr['successor_IP'] + '/query'), json= json.dumps({'key':data['key']}))
        else:
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
                        return(requests.post(str('http://' + addr['successor_IP'] + '/query'), json= json.dumps({'key':data['key']})).text)
            elif info['method'] == 'lin':
                if Keepthekey(hashed_key):
                    requests.post(str('http://' + addr['successor_IP'] + '/query_replica'), json= json.dumps({'key':data['key']
                                    , 'replication_factor' : replication_factor - 1}))
                    if hashed_key in files:
                        return(str(data['key']+':'+files[hashed_key][1]))
                    else: 
                        return "Hi"
                else:
                    print('I am not responsible for the key you asked')
                    print('Forwarding the request...')
                    return(requests.post(str('http://' + addr['successor_IP'] + '/query'), json= json.dumps({'key':data['key']})).text)
                    
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
                return("Hi")
        else:
            if info['method'] == 'lin':
                requests.post(str('http://' + addr['successor_IP'] + '/query_replica'), json= json.dumps({'key':data['key']
                                    , 'replication_factor' : data['replication_factor'] - 1}))
   
    return "Hi"

@app.route('/query_star', methods = ['POST', 'GET'])
def query_star():
    if request.method == 'POST':
        global addr
        data = json.loads(request.json)
        if data['myIP'] == myIp and data['rep'] != 0:
            return "Hi"
        else:
            print('Printing all the files i have due to query key...\n')
            print("{:<20} {:<7}".format('Key','Value'))
            for v in files.values():
                key, val, _ = v #_ runs only for linearizability
                print("{:<20} {:<7}".format(key, val))
            print('')
            requests.post(str('http://' + addr['successor_IP'] + '/query_star'), json = json.dumps({'myIP' : data['myIP'], 'rep' : 1}))
    return "Hi"


@app.route('/insert', methods = ['POST', 'GET']) #insert [key, value] to the chosen node
def insert(): 
    replication_factor = info['replication_factor']
    if request.method == 'POST':
        data = json.loads(request.json) #pairnw key kai value
        print('Key given is : ', data['key'])
        print('Value for that key is : ', data['value'])
        hash_value = hashlib.sha1()
        hash_value.update(data['key'].encode())
        tmp = hash_value.hexdigest()
        hashed_key = int(tmp, 16)

        if replication_factor == 1:
            if Keepthekey(hashed_key):
                if hashed_key not in files:
                    files[hashed_key] = [data['key'], data['value'], replication_factor]
                print('Files in this node : \n')
                print("{:<15} {:<20} {:<7}".format('Hashed_Key','Key','Value'))
                for k,v in files.items():
                    key, val, _ = v
                    print("{:<15} {:<20} {:<7}".format(k, key, val))
                print('')
            else:
                print('Key doesnt belong to this node')
                print('Forwarding the key value pair to the successor...\n')
                requests.post(str('http://' + addr['successor_IP'] + '/insert'), json = json.dumps({'key' : data['key'], 'value' : data['value']}))
                return "Hi"
        else:
            if Keepthekey(hashed_key):
                data['replication_factor'] = replication_factor
                files[hashed_key] = [data['key'], data['value'], data['replication_factor']]
                print('Files in this node : \n')
                print("{:<15} {:<20} {:<7}".format('Hashed_Key','Key','Value'))
                for k,v in files.items():
                    key, val, _ = v
                    print("{:<15} {:<20} {:<7}".format(k, key, val))
                print('')
                if info['method'] == 'lin':
                    requests.post(str('http://' + addr['successor_IP'] + '/insert_replicas'), json = json.dumps({'key' : data['key'], 
                    'value' : data['value'], 'replication_factor' : data['replication_factor'] - 1}))
                elif info['method'] == 'even':
                    evcon(str('http://' + addr['successor_IP'] + '/insert_replicas'), json.dumps({'key' : data['key'], 
                    'value' : data['value'], 'replication_factor' : data['replication_factor'] - 1}))    
                    return "Hi"
            else:
                print('Key doesnt belong to this node')
                print('Forwarding the key value pair to the successor...\n')
                requests.post(str('http://' + addr['successor_IP'] + '/insert'), json = json.dumps({'key' : data['key'], 'value' : data['value']}))
                return "Hi"

    return "Hi"

@app.route('/insert_replicas', methods = ['POST', 'GET']) #insert the [key,value] replicas to the k-1 successors 
def insert_replicas():
    if request.method == 'POST':
        data = json.loads(request.json)
        replication_factor = data['replication_factor']
        if replication_factor != 0:
            print('Key_replica given is : ', data['key'])
            print('Value_replica for that key is : ', data['value'])
            hash_value = hashlib.sha1()
            hash_value.update(data['key'].encode())
            tmp = hash_value.hexdigest()
            hashed_key = int(tmp, 16) 
            files[hashed_key] = [data['key'], data['value'], replication_factor]
            if info['method'] == 'lin':
                requests.post(str('http://' + addr['successor_IP'] + '/insert_replicas'), json = json.dumps({'key' : data['key'],     
                'value' : data['value'], 'replication_factor' : replication_factor - 1}))
            elif info['method'] == 'even':
                evcon(str('http://' + addr['successor_IP'] + '/insert_replicas'), json.dumps({'key' : data['key'],     
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

        if replication_factor == 1: #if we dont have replicas
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
                        key, val, _ = v
                        print("{:<15} {:<20} {:<7}".format(k, key, val))
                    print('')
                else:
                    print('There are no files in this node')
            else:
                print('Key doesnt belong to this node')
                print('Forwarding the key to the successor...\n')
                requests.post(str('http://' + addr['successor_IP'] + '/delete'), json = json.dumps({'key' : data['key']}))
        else: # if we have replicas 
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
                        key, val, _ = v
                        print("{:<15} {:<20} {:<7}".format(k, key, val))
                    print('')
                else:
                    print('There are no files in this node')
                if info['method'] == 'lin':
                    requests.post(str('http://' + addr['successor_IP'] + '/delete_replicas'), json = json.dumps({'key' : data['key'],
                                                'replication_factor' : replication_factor - 1}))
                elif info['method'] == 'even':
                    evcon(str('http://' + addr['successor_IP'] + '/delete_replicas'), json.dumps({'key' : data['key'],
                                                'replication_factor' : replication_factor - 1}))
            else: 
                print('Key doesnt belong to this node')
                print('Forwarding the key to the successor...\n')
                requests.post(str('http://' + addr['successor_IP'] + '/delete'), json = json.dumps({'key' : data['key']}))
    
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
                    requests.post(str('http://' + addr['successor_IP'] + '/delete_replicas'), json = json.dumps({'key' : data['key'], 
                'value' : data['value'], 'replication_factor' : replication_factor - 1}))
                elif info['method'] == 'even':
                    evcon(str('http://' + addr['successor_IP'] + '/delete_replicas'),json.dumps({'key' : data['key'], 
                'value' : data['value'], 'replication_factor' : replication_factor - 1}))
            else:
                if info['method'] == 'lin':
                    requests.post(str('http://' + addr['successor_IP'] + '/delete_replicas'), json = json.dumps({'key' : data['key'], 'replication_factor' : replication_factor - 1}))
                elif info['method'] == 'even':
                    evcon(str('http://' + addr['successor_IP'] + '/delete_replicas'), json.dumps({'key' : data['key'], 'replication_factor' : replication_factor - 1}))
            return "Hi"
    return "Hi"


@app.route('/overlay', methods = ['POST', 'GET']) 
def overlay():
    if request.method == 'POST':
        print('Request to print the overlay of the netword')
        print('Passing the request to boostrap...')
        addrs = 'http://192.168.0.1:5000/overlay'
        requests.post(addrs)
        return "Hi"

#https://networklore.com/start-task-with-flask/
def start_runner():
    def start_loop():
        not_started = True
        while not_started:
            flag = 0
            print('In start loop')
            try:
                r = requests.get('http://'+ myIp +'/')
                if r.status_code == 404:
                    print('Server started, quiting start_loop')
                    not_started = False
                    flag = 1
                    addrs = {'ip' : Ip, 'port' : Port}
                    print(addrs)
                    requests.post('http://192.168.0.1:5000/join', json = json.dumps(addrs)).content()
                    return
            except:
                if flag != 1:
                    print('Server not yet started')
            time.sleep(5)
    print('Started runner')
    thread = threading.Thread(target=start_loop)
    thread.start()

        

if __name__ == '__main__':
  start_runner()
  app.run(host = Ip, port = Port)

