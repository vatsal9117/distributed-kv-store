"""
RPC Communication Layer for Raft
Handles network communication between nodes
"""

import socket
import json
import threading
from typing import Optional, Dict, Any

class RPCServer:
    def __init__(self, host: str, port: int, node):
        self.host = host
        self.port = port
        self.node = node
        self.server_socket = None
        self.running = False
        
    def start(self):
        """Start RPC server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"RPC Server started on {self.host}:{self.port}")
        
        # Accept connections in separate thread
        threading.Thread(target=self._accept_connections, daemon=True).start()
    
    def _accept_connections(self):
        """Accept incoming connections"""
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True
                ).start()
            except Exception as e:
                if self.running:
                    print(f"Error accepting connection: {e}")
    
    def _handle_client(self, client_socket: socket.socket):
        """Handle client request"""
        try:
            # Receive data
            data = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break
            
            if not data:
                return
            
            # Parse request
            request = json.loads(data.decode())
            method = request.get('method')
            
            # Dispatch to appropriate handler
            if method == 'RequestVote':
                response = self._handle_request_vote(request)
            elif method == 'AppendEntries':
                response = self._handle_append_entries(request)
            elif method == 'ClientRequest':
                response = self._handle_client_request(request)
            else:
                response = {'success': False, 'error': 'Unknown method'}
            
            # Send response
            response_data = json.dumps(response).encode() + b"\n"
            client_socket.sendall(response_data)
            
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()
    
    def _handle_request_vote(self, request: dict) -> dict:
        """Handle RequestVote RPC"""
        params = request['params']
        term, vote_granted = self.node.request_vote(
            params['term'],
            params['candidate_id'],
            params['last_log_index'],
            params['last_log_term']
        )
        return {
            'term': term,
            'vote_granted': vote_granted
        }
    
    def _handle_append_entries(self, request: dict) -> dict:
        """Handle AppendEntries RPC"""
        params = request['params']
        
        # Reconstruct LogEntry objects
        from enhanced_raft_node import LogEntry
        entries = [
            LogEntry(
                term=e['term'],
                command=e['command'],
                index=e['index']
            ) for e in params['entries']
        ]
        
        term, success = self.node.append_entries(
            params['term'],
            params['leader_id'],
            params['prev_log_index'],
            params['prev_log_term'],
            entries,
            params['leader_commit']
        )
        return {
            'term': term,
            'success': success
        }
    
    def _handle_client_request(self, request: dict) -> dict:
        """Handle client request"""
        params = request['params']
        success, message = self.node.client_request(
            params['operation'],
            params['key'],
            params.get('value')
        )
        return {
            'success': success,
            'message': message
        }
    
    def stop(self):
        """Stop RPC server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()


class RPCClient:
    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout
    
    def call(self, host: str, port: int, method: str, params: dict) -> Optional[dict]:
        """Make RPC call to remote node"""
        try:
            # Create socket
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.settimeout(self.timeout)
            
            # Connect
            client_socket.connect((host, port))
            
            # Send request
            request = {
                'method': method,
                'params': params
            }
            request_data = json.dumps(request).encode() + b"\n"
            client_socket.sendall(request_data)
            
            # Receive response
            data = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break
            
            # Parse response
            response = json.loads(data.decode())
            client_socket.close()
            
            return response
            
        except socket.timeout:
            print(f"RPC call to {host}:{port} timed out")
            return None
        except Exception as e:
            print(f"RPC call failed: {e}")
            return None
    
    def request_vote(self, host: str, port: int, term: int, candidate_id: str,
                    last_log_index: int, last_log_term: int) -> Optional[dict]:
        """Send RequestVote RPC"""
        return self.call(host, port, 'RequestVote', {
            'term': term,
            'candidate_id': candidate_id,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term
        })
    
    def append_entries(self, host: str, port: int, term: int, leader_id: str,
                      prev_log_index: int, prev_log_term: int, entries: list,
                      leader_commit: int) -> Optional[dict]:
        """Send AppendEntries RPC"""
        # Serialize LogEntry objects
        serialized_entries = [
            {
                'term': e.term,
                'command': e.command,
                'index': e.index
            } for e in entries
        ]
        
        return self.call(host, port, 'AppendEntries', {
            'term': term,
            'leader_id': leader_id,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': serialized_entries,
            'leader_commit': leader_commit
        })
    
    def client_request(self, host: str, port: int, operation: str, 
                      key: str, value: str = None) -> Optional[dict]:
        """Send client request"""
        params = {
            'operation': operation,
            'key': key
        }
        if value is not None:
            params['value'] = value
        
        return self.call(host, port, 'ClientRequest', params)