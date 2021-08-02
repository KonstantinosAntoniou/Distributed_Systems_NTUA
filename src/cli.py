import click
from click.termui import prompt
import requests
from flask import Flask, request
import os
import json
import random
import sys
import time


IPs = ['192.168.0.1:5000', '192.168.0.2:5000','192.168.0.3:5000', '192.168.0.4:5000', '192.168.0.5:5000',
       '192.168.0.1:2000', '192.168.0.2:2000', '192.168.0.3:2000', '192.168.0.4:2000', '192.168.0.5:2000'] 

@click.group()
def cli():
    pass

@cli.command()
def help():
    print('-----------------  Available commands are listed below :  -----------------')
    print('')
    print('insert <key> <value> <node>'.center(os.get_terminal_size().columns))
    print('Given a (key, value) pair, where key is the song name and value is a string, insert this pair to the node provided. Node has either to be random or a specific ip and port address.\n')
    print('delete <key> <node>'.center(os.get_terminal_size().columns))
    print('Given a specific key delete the (key, value) pair. Parameter node has either to be random or a specified node.\n')
    print('query <key> <node>'.center(os.get_terminal_size().columns))
    print('Given a key search for that key and return the value attached to that key starting from a random node or a specified one (parameter node). Using the character * instead of a certain key, print all <key,value> pairs saved to the DHT for each node.\n')
    print('depart <node>'.center(os.get_terminal_size().columns))
    print('Given the IP address of a node this node leaves the chord.\n')
    print('overlay <node>'.center(os.get_terminal_size().columns))
    print('Prints all nodes that belong to the chord following the order that these nodes are connected.')
    print('file <filename>'.center(os.get_terminal_size().columns))
    print('Extra command in case you want to read from one of the three files given')



@click.option('--value', type = str, prompt = True, help = 'Give the value corresponding to the key')
@click.option('--key', type = str, prompt = True, help = 'Give the key')
@click.option('--node', type= str, prompt = True, help = 'Either give random or ip and port to send the request')
@cli.command()
def insert(key, value, node):

    if node == 'random':
        addr = random.choice(IPs)
        print('The request for inserting will be delivered to the node with an address of ', addr)
        f_addr = str('http://' + addr + '/insert')
        print('Sending the request...')
        requests.post(f_addr, json = json.dumps({'key' : key, 'value' : value}))
    else:
        if node in IPs:
            f_addr = str('http://' + node + '/insert')
            print('Reminder : You chose a specific node')
            print('The request for inserting will be delivered to the node with an address of ', node)
            print('Sending the request...')
            requests.post(f_addr, json = json.dumps({'key' : key, 'value' : value}))
        else:
            print('The address given isnt valid')
    
    return

@click.option('--key', type = str, prompt = True, help = 'Give the key')
@click.option('--node', type= str, prompt = True, help = 'Either give random or ip and port to send the request')
@cli.command()
def query(key, node):
    
    if node == 'random':
        addr = random.choice(IPs)
        if key != '*':
            print('The request for finding the value of the key ', key, 'will be delivered to the node with an address of ', addr )
            f_addr = str('http://' + addr + '/query')
            print('Sending the request...')
            requests.post(f_addr, json = json.dumps({'key' : key}))
        else:
            print('You gave * for the key so all the nodes should print all the <key, value> pairs' )
            f_addr = str('http://' + addr + '/query_star')
            print('Sending the request...')
            requests.post(f_addr, json = json.dumps({'myIP' : addr, 'rep' : 0}))
    else:
        if node in IPs:
            if key != '*':
                f_addr = str('http://' + node + '/query')
                print('Reminder : You chose a specific node')
                print('The request for finding the value of the key ', key, 'will be delivered to the node with an address of ', node)
                print('Sending the request...')
                requests.post(f_addr, json = json.dumps({'key' : key}))
            else:
                f_addr = str('http://' + node + '/query_star')
                print('Reminder : You chose a specific node')
                print('You gave * for the key so all the nodes should print all the <key, value> pairs' )
                print('Sending the request...')
                requests.post(f_addr, json = json.dumps({'myIP' : node, 'rep' : 0}))
        else:
            print('The address given isnt valid')


@click.option('--key', type = str, prompt = True, help = 'Give the key')
@click.option('--node', type= str, prompt = True, help = 'Either give random or ip and port to send the request')
@cli.command()
def delete(key, node):

    if node == 'random':
        addr = random.choice(IPs)
        print('The request for deleting will be delivered to the node with an address of ', addr)
        f_addr = str('http://' + addr + '/delete')
        print('Sending the request...')
        requests.post(f_addr, json = json.dumps({'key' : key}))
    else:
        if node in IPs:
            f_addr = str('http://' + node + '/delete')
            print('Reminder : You chose a specific node')
            print('The request for deleting will be delivered to the node with an address of ', node)
            print('Sending the request...')
            requests.post(f_addr, json = json.dumps({'key' : key}))
        else:
            print('The address given isnt valid')
    
    return


@click.option('--port', type = str, prompt = True, help='Give the Port of the node to be departed')
@click.option('--ip', type = str, prompt = True, help='Give the IP of the node to be departed')
@cli.command()
def depart(ip, port):
    check = str(ip + ':' + port)
    if check in IPs:
        addr = str('http://' + ip + ':' +  port + '/depart')  #send to yourself and then pass to bootstrap
        print('Node with an Ip of ',ip,'listening to the port ',port,'needs to depart...')
        print('Sending the request...')
        requests.post(addr, json = json.dumps({'ip' : ip, 'port' : port}))
    else:
        print('Give a valid Ip and Port')
    return "Hi"


@click.option('--port', type = str, prompt = True, help='Give the Port of the node to to which to send request for overlay')
@click.option('--ip', type = str, prompt = True, help='Give the IP of the node to which to send request for overlay')
@cli.command()
def overlay(ip,port):
    check = str(ip + ':' + port)
    if check in IPs:
        addr = str('http://' + ip + ':' +  port + '/overlay') #send to yourself and then pass to bootstrap because he is the only one that knows all the nodes of the chord
        print('Sending the request to print the overlay of the chord...')
        requests.post(addr)
    else:
        print('Give a valid Ip and Port')
    return "Hi"

@click.option('--file', type = str, prompt = True, help = 'Give either insert / query / requests in order to read from the right file')
@cli.command()
def Readfile(file):

    counter = 0

    if file == 'insert.txt':
        print('I will be inseting all the <key, value> pairs found in this file')
        with open('insert.txt', 'r') as reader:
            line = reader.readline()
            while line != '':
                counter += 1
                print('Sending request for inseting No.',counter)
                key,value = line.split(',')
                print('Sending', key, 'with a value of', value)
                addr = random.choice(IPs)
                f_addr = str('http://' + addr + '/insert')
                if counter == 1:
                    starting_time = time.time()
                requests.post(f_addr, json = json.dumps({'key' : key, 'value' : value}))
                line = reader.readline()
            print('Insert.txt write throughput :')
            print("--- %s seconds ---" % ((time.time() - starting_time)/counter))

    
    if file == 'query.txt':
        print('I will be all the keys mentioned in this file')
        with open('query.txt', 'r') as reader: 
            line = reader.readline()
            while line != '':
                counter += 1
                print('Sending request for query No.',counter)
                key = line.split('\n')[0]
                print('Searching key:', key)
                addr = random.choice(IPs)
                f_addr = str('http://' + addr + '/query')
                if counter == 1:
                    starting_time = time.time()
                requests.post(f_addr, json = json.dumps({'key' : key}))
                line = reader.readline()
            print('Query.txt read throughput :')
            print("--- %s seconds ---" % ((time.time() - starting_time)/counter))

    if file == 'test.txt':
        print('I will be inseting all the <key, value> pairs found in this file')
        with open('test.txt', 'r') as reader:
            line = reader.readline()
            while line != '':
                counter += 1
                print('Sending request for inseting No.',counter)
                key,value = line.split(',')
                print('Sending', key, 'with a value of', value)
                addr = random.choice(IPs)
                f_addr = str('http://' + addr + '/insert')
                requests.post(f_addr, json = json.dumps({'key' : key, 'value' : value}))
                line = reader.readline()

    if file == 'requests.txt':
        print('Request.txt is executing...')
        with open('requests.txt', 'r') as reader:
            line = reader.readline()
            while line != '':
                counter += 1
                command = line.split(',')[0]
               # print('Command chosen : ', command)
                addr = random.choice(IPs)
                if command == 'query':
                    key = line.split(',')[1].replace('\n','')
                    if key.startswith(" ") : key = key[1:]
                    print('Key for query No.', counter, 'is : ', key)
                    f_addr = str('http://' + addr + '/query')
                    print(str(str(counter)+'.'+requests.post(f_addr, json = json.dumps({'key' : key})).text))
                elif command == 'insert':
                    key, value = line.split(',')[1].replace('\n',''), line.split(',')[2].replace('\n','')
                    if key.startswith(" ") : key = key[1:]
                    if value.startswith(" ") : value = value[1:]
                    f_addr = str('http://' + addr + '/insert')
                    print('Key for insert No.', counter, 'is : ', key, 'and value is :', value)
                    requests.post(f_addr, json = json.dumps({'key' : key, 'value' : value}))
                else:
                    print('Something went terribly wrong')
                line = reader.readline()

    if file not in ['insert.txt', 'query.txt', 'requests.txt', 'test.txt']:
        print('Wrong file name')

if __name__ == '__main__':
    cli()
        
