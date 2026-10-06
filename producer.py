import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_cisco_command(router_ip, username, password, command):
    payload = {
        "router_ip": router_ip,
        "username": username,
        "password": password,
        "command": command
    }
    producer.send('cisco-commands', payload)
    producer.flush()
    print(f"[Backend] Sent command '{command}' for {router_ip}")

if __name__ == "__main__":
    send_cisco_command("192.168.200.10", "admin", "cisco", "show ip interface brief")
    send_cisco_command("192.168.200.20", "admin", "cisco", "show ip interface brief")
