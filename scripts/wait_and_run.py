"""
Simple wait-for script that blocks until TCP services are available, then runs a command.
Usage:
    python scripts/wait_and_run.py host1:port host2:port -- command arg1 arg2
"""
import sys
import socket
import time
import subprocess


def wait_for_host(host: str, port: int, timeout: int = 60) -> bool:
    start = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except Exception:
            if time.time() - start > timeout:
                return False
            time.sleep(1)


def main():
    if '--' not in sys.argv:
        print(
            'Usage: wait_and_run.py host:port [host:port ...] -- command [args...]')
        sys.exit(2)

    idx = sys.argv.index('--')
    services = sys.argv[1:idx]
    cmd = sys.argv[idx+1:]

    for svc in services:
        if ':' not in svc:
            print(f'Skipping invalid service entry: {svc}')
            continue
        host, port = svc.split(':', 1)
        port = int(port)
        print(f'Waiting for {host}:{port}...')
        ok = wait_for_host(host, port, timeout=120)
        if not ok:
            print(f'Timeout waiting for {host}:{port}', file=sys.stderr)
            sys.exit(1)
        print(f'{host}:{port} is available')

    if not cmd:
        print('No command provided to run after wait')
        sys.exit(2)

    print('Starting command:', ' '.join(cmd))
    # Run the command and stream output
    p = subprocess.Popen(cmd)
    p.wait()
    sys.exit(p.returncode)


if __name__ == '__main__':
    main()
