import socket
import json
import argparse
import sys

# Change this to your Server VM's IP
SERVER_IP = '192.168.56.106'
PORT = 65432

def send_request(request_data):
    """Connects to the server, sends a JSON request, and prints the response."""
    try:
        # Create a TCP socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Set a timeout so the client doesn't hang forever if the server is down
            s.settimeout(5.0)
            s.connect((SERVER_IP, PORT))
            
            # Send the data
            s.sendall(json.dumps(request_data).encode('utf-8'))
            
            # Receive the response
            data = s.recv(1024)
            if not data:
                print("No response from server.")
                return

            response = json.loads(data.decode('utf-8'))
            print(f"Response: {json.dumps(response, indent=2)}")
            
    except ConnectionRefusedError:
        print(f"Error: Connection refused. Is the server running on {SERVER_IP}:{PORT}?")
    except socket.timeout:
        print("Error: Connection timed out.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Banking Client')
    parser.add_argument('action', choices=['balance', 'deposit', 'withdraw', 'malformed', 'broken_json'])
    parser.add_argument('--account', type=str, default='A1001')
    parser.add_argument('--amount', type=float)

    args = parser.parse_args()

    # Handle deliberate errors for the demo requirements
    if args.action == 'broken_json':
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((SERVER_IP, PORT))
            s.sendall(b"{ broken json random bytes")
            print(f"Response: {s.recv(1024).decode('utf-8')}")
        sys.exit(0)

    if args.action == 'malformed':
        req = {"action": "withdraw"} # Missing account and amount
    else:
        req = {"action": args.action, "account": args.account}
        if args.amount is not None:
            req["amount"] = args.amount

    print(f"Sending: {req}")
    send_request(req)
