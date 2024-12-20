import time
from flask import Flask, request, jsonify
import threading
import requests
import sys

app = Flask(__name__)

BASE_PORT = 6666
REPLICAS = list(range(3))
SYNC_INTERVAL = 3
TIMEOUT = 1

class Node():

    def __init__(self, node_id):
        self.node_id = node_id
        self.db = {}  
        self.vector_clock = {}
        self.times = {}
        self.replicas = REPLICAS

        threading.Thread(target=self.sync, daemon=True).start()


    def merge(self, data):
        remote_state = data['state']
        remote_vector_clock = data['vector_clock']
        times = data['times']

        for key, remote_entry in remote_state.items():
            remote_clock = remote_vector_clock[key]
            if not self.db.get(key) or self.compare_clocks(remote_clock, self.vector_clock.get(key, {})) == 1 or times[key] > self.times[key]:
                self.db[key] = remote_entry
                self.vector_clock[key] = remote_clock
                self.times[key] = times[key]


    def compare_clocks(self, remote, current):
        remote_actual = False
        for key in set(remote.keys()).union(current.keys()):
            result = remote.get(key, 0) - current.get(key, 0)
            if result > 0: remote_actual = True
            elif result < 0: return -1
        return 1 if remote_actual else 0


    def get_blocked(self):
        line = open('block').read().split()
        return list(map(int, line)) if len(line) > 0 else []


    def broadcast(self, endpoint, data):
        if self.node_id in self.get_blocked(): return
        for replica in self.replicas:
            if replica != self.node_id and replica not in self.get_blocked():
                try:
                    requests.post(f'http://localhost:{BASE_PORT + replica}{endpoint}', json=data, timeout=TIMEOUT)
                except requests.exceptions.RequestException:
                    pass


    def sync(self):
        while True:
            if self.node_id in self.get_blocked(): continue
            for replica in self.replicas:
                if replica != self.node_id and replica not in self.get_blocked():
                    try:
                        response = requests.get(f'http://localhost:{BASE_PORT + replica}/', timeout=TIMEOUT)
                        self.merge(response.json())
                        print(response.json())
                    except requests.exceptions.RequestException:
                        pass
            time.sleep(SYNC_INTERVAL)


node: Node = None


@app.route('/update', methods=['PATCH'])
def patch():
    replica_id = request.host
    for key, value in request.json['updates'].items():
        node.vector_clock.setdefault(key, {})
        node.vector_clock[key][replica_id] = node.vector_clock[key].get(replica_id, 0) + 1
        node.db[key] = value
        node.times[key] = time.time()

    node.broadcast('/sync', {'state': node.db, 'vector_clock': node.vector_clock, 'times': node.times})

    return jsonify({'status': 'ok'})


@app.route('/sync', methods=['POST'])
def sync():
    node.merge(request.json)
    return jsonify({'status': 'ok'})


@app.route('/', methods=['GET'])
def get_state():
    return jsonify({'state': node.db, 'vector_clock': node.vector_clock, 'times': node.times})


if __name__ == '__main__':
    node = Node(int(sys.argv[1]))
    app.run(host='0.0.0.0', port=BASE_PORT + node.node_id)