import json
import time
import subprocess
from kafka import KafkaConsumer

def execute_ssh_command(ip, username, password, command):
    print(f"\n[Worker] Connecting to {ip} via Native SSH...")
    
    ssh_cmd = [
        "sshpass", "-p", password,
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "KexAlgorithms=+diffie-hellman-group1-sha1,diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1",
        "-o", "HostKeyAlgorithms=+ssh-rsa",
        "-o", "PubkeyAcceptedAlgorithms=+ssh-rsa",
        "-o", "Ciphers=+aes128-cbc,3des-cbc,aes256-cbc,aes128-ctr,aes256-ctr",
        f"{username}@{ip}",
        command
    ]
    
    try:
        result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            print(f"[Worker] Output from {ip}:\n{'-'*40}\n{result.stdout.strip()}\n{'-'*40}")
        else:
            print(f"[Worker] SSH Error from {ip}: {result.stderr.strip()}")
    except Exception as e:
        print(f"[Worker] Error executing command on {ip}: {e}")

def start_worker():
    consumer = KafkaConsumer(
        'cisco-commands',
        bootstrap_servers=['localhost:9092'],
        group_id=f'router-executors-{int(time.time())}',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("[Worker] Waiting for NEW Cisco command events from Kafka...")
    for message in consumer:
        data = message.value
        print(f"[Worker] Received event: {data}")
        execute_ssh_command(
            ip=data.get('router_ip'),
            username=data.get('username'),
            password=data.get('password'),
            command=data.get('command')
        )

if __name__ == "__main__":
    start_worker()
