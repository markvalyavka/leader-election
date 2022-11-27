#!/usr/bin/env python3
"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import logging
import json
import threading
import requests
import argparse


logging.basicConfig(level=logging.INFO)

node_id = ""
N = NN = P = L = ""
cluster_nodes = []
nick = ""


def join_node(joined_node_id):
    global P
    requests.post(f"http://0.0.0.0:{joined_node_id}", json={
        "msg_type": "join",
        "from": node_id,
    })
    P = joined_node_id


def handle_join(params, from_):
    global N, NN, P
    single_node_cluster = N == P == NN
    # Step 1
    prev_N = N
    prev_NN = NN
    NN = N
    N = from_

    # Step 2
    requests.post(f"http://0.0.0.0:{from_}", json={
        "msg_type": "join_reply",
        "from": node_id,
        "params": {
            "N": prev_N,
            "NN": from_ if single_node_cluster else prev_NN,
            "L": L,
        }
    })
    # Step 3
    if not single_node_cluster:
        requests.post(f"http://0.0.0.0:{P}", json={
            "msg_type": "change_nn",
            "from": node_id,
            "params": {
                "NN": from_
            }
        })

    # Step 4
    if single_node_cluster:
        P = from_
    else:
        requests.post(f"http://0.0.0.0:{prev_N}", json={
            "msg_type": "change_p",
            "from": node_id,
            "params": {
                "P": from_
            }
        })

    # Step 5
    if node_id == L:
        cluster_nodes.append(from_)
    else:
        requests.post(f"http://0.0.0.0:{L}", json={
            "msg_type": "register_node",
            "from": node_id,
            "params": {
                "new_node": from_
            }
        })


def handle_register_node(params, from_):
    if node_id == L:
        cluster_nodes.append(params["new_node"])

def handle_deregister_node(params, from_):
    if node_id == L:
        print("Removed node:", params["removed_node"])
        cluster_nodes.remove(params["removed_node"])

def handle_change_p(params, from_):
    global P
    P = params["P"]

def handle_change_nn(params, from_):
    global NN
    NN = params["NN"]

def handle_get_n(params, from_):
    return

def handle_join_reply(params, from_):
    global N, NN, L
    N = params["N"]
    NN = params["NN"]
    L = params["L"]
    # logging.info(f"I received join_reply with N - {my_N}, NN - {my_NN}, L - {my_L}!")

def handle_log_state(params, from_):
    logging.info(f"[Node -{node_id}]: N - {N}, NN - {NN}, P - {P}, L - {L}, cluster - {cluster_nodes}!")

def handle_log_chat_msg(params, from_):
    logging.info(f"[{params['sender']}]: {params['chat_msg']}\n")

def handle_send_chat_msg(params, from_):
    if node_id == L:
        logging.info(f"[{from_}]: {params['chat_msg']}\n")
        unreachable_node = None
        for node in cluster_nodes:
            try:
                if node != from_:
                    requests.post(f"http://0.0.0.0:{node}", json={
                        "msg_type": "log_chat_msg",
                        "from": node_id,
                        "params": {
                            "sender": from_,
                            "chat_msg": params["chat_msg"]
                        }
                    })
            except requests.ConnectionError:
                unreachable_node = node
        if unreachable_node is not None:
            if unreachable_node == N:
                remove_n_and_repair(params, from_)
            else:
                requests.post(f"http://0.0.0.0:{N}", json={
                    "msg_type": "dead_node_detected",
                    "from": node_id,
                    "params": {
                        "dead_node": unreachable_node
                    }
                })

    elif node_id != L:
        logging.info(f"[{node_id}]: {params['chat_msg']}\n")
        requests.post(f"http://0.0.0.0:{L}", json={
            "msg_type": "send_chat_msg",
            "from": node_id,
            "params": {
                "chat_msg": params["chat_msg"]
            }
        })

def handle_dead_node_detected(params, from_):
    dead_node = params["dead_node"]
    if N == dead_node:
        print(f"Repairing on {node_id}")
        remove_n_and_repair(params, from_)
    else:
        requests.post(f"http://0.0.0.0:{N}", json={
            "msg_type": "dead_node_detected",
            "from": node_id,
            "params": {
                "dead_node": dead_node
            }
        })


def remove_n_and_repair(params, from_):
    global N, NN, P, L, cluster_nodes
    # Step 0
    requests.post(f"http://0.0.0.0:{L}", json={
        "msg_type": "deregister_node",
        "from": node_id,
        "params": {
            "removed_node": N
        }
    })
    # Step 1
    prev_N = N
    prev_NN = NN
    N = NN

    if NN == node_id:
        N = NN = P = L = node_id
        cluster_nodes = []
        return

    # Step 2
    my_new_NN = requests.get(f"http://0.0.0.0:{NN}/n").text
    NN = int(my_new_NN)

    requests.post(f"http://0.0.0.0:{N}", json={
        "msg_type": "change_p",
        "from": node_id,
        "params": {
            "P": node_id
        }
    })

    # Step 3
    requests.post(f"http://0.0.0.0:{P}", json={
        "msg_type": "change_nn",
        "from": node_id,
        "params": {
            "NN": N
        }
    })


class NodeRequestHandler(BaseHTTPRequestHandler):
    def _set_response_headers(self, content_type='text/html'):
        self.send_response(200)
        self.end_headers()

    def do_GET(self):
        if self.path == "/n":
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(f"{N}".encode('utf-8'))
        else:
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            cur_state = {
                "Node": node_id,
                "N": N,
                "NN": NN,
                "P": P,
                "L": L,
                "cluster_nodes": cluster_nodes
            }
            self.wfile.write(json.dumps(cur_state).encode('utf-8'))
            # self.wfile.write(f"[Node - {node_id}]: N - {N}, NN - {NN}, P - {P}, L - {L}, cluster - {cluster_nodes}!".encode('utf-8'))

    def do_POST(self):
        # Read POST body.
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        self._set_response_headers()
        # Unmarshal to dict.
        post_data = json.loads(post_data)
        msg_type = post_data["msg_type"]
        from_ = post_data.get("from", "")
        params = post_data.get("params", {})
        print(f"Received {msg_type}")
        # Handle message.
        msg_type_to_handler = {
            "join": handle_join,
            "join_reply": handle_join_reply,
            "change_p": handle_change_p,
            "log_state": handle_log_state,
            "change_nn": handle_change_nn,
            "register_node": handle_register_node,
            "deregister_node": handle_deregister_node,
            "send_chat_msg": handle_send_chat_msg,
            "log_chat_msg": handle_log_chat_msg,
            "remove_next_from_outside": remove_n_and_repair,
            "dead_node_detected": handle_dead_node_detected,
        }
        msg_type_to_handler[msg_type](params, from_)

def run(server_class=ThreadingHTTPServer, handler_class=NodeRequestHandler, port=8080):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info(f'[STARTING] A node at {httpd.server_address[0]}:{httpd.server_address[1]}.')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Stopping httpd...')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', help="start node with port", type=int)
    parser.add_argument('--join', '-join', help="port of a node to join", type=int)
    parser.add_argument('--nick', '-nick', help="your nickname in the chat", type=str)

    # Parse arguments.
    cli_args = parser.parse_args()
    node_id = cli_args.port
    joined_node_id = cli_args.join
    nick = cli_args.nick or "unknown"
    N = NN = P = L = node_id

    if node_id is None and joined_node_id is None:
        parser.error("Not enough arguments.")

    if node_id and joined_node_id:
        # Run a thread that joins a cluster after its own server is started.
        threading.Timer(3, join_node, args=[joined_node_id]).start()
    # threading.Timer(3, handle_test, args=[{}, 1]).start()
    run(port=node_id)







