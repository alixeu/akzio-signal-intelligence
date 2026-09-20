"""Loopback-only integration check for the native Observer transport."""
import http.server
import json
import subprocess
import sys
import threading


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        # 307 目标记录是否收到请求；测试要求认证请求不能跟随 redirect 离开原 endpoint。
        self.server.requests += 1
        if self.path.startswith('/redirect/'):
            self.send_response(307)
            self.send_header('Location', f'http://127.0.0.1:{self.server.server_port}/escaped')
            self.end_headers()
            return
        if self.path == '/escaped':
            self.server.escaped += 1
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps({'enabled': False, 'store_identity': None, 'runs': []}).encode())

    def log_message(self, *_):
        pass


with http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
    server.escaped = 0
    server.requests = 0
    threading.Thread(target=server.serve_forever, daemon=True).start()
    result = subprocess.run([sys.argv[1], '--transport-check', f'http://127.0.0.1:{server.server_port}'])
    server.shutdown()
    if server.escaped:
        raise SystemExit(f'FAIL: redirect target received {server.escaped} authenticated requests')
    if not server.requests:
        raise SystemExit('FAIL: transport check made no HTTP requests')
    raise SystemExit(result.returncode)
