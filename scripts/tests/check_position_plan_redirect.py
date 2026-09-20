"""Exercise the launcher's actual HTTP entrypoint without Core or providers."""
import http.server
import importlib.util
import sys
from pathlib import Path
import threading
from urllib.error import HTTPError
from urllib.request import Request

repo = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('position_plan_run', repo / 'scripts/position_plan_run.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        # 直接响应应正常返回；redirect 响应只被观察，不允许 token 随 Location 转发。
        if self.path == '/redirect':
            self.send_response(307)
            self.send_header('Location', f'http://127.0.0.1:{self.server.server_port}/escaped')
            self.end_headers()
            return
        if self.path == '/escaped':
            self.server.escaped += 1
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"runs":[]}')

    def log_message(self, *_):
        pass


with http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
    server.escaped = 0
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f'http://127.0.0.1:{server.server_port}'
    with module.urlopen(Request(base + '/direct', headers={'x-akzio-token': 'fixture-token'}), timeout=3) as response:
        assert response.status == 200
        assert response.read() == b'{"runs":[]}'
    rejected = False
    try:
        with module.urlopen(Request(base + '/redirect', headers={'x-akzio-token': 'fixture-token'}), timeout=3) as response:
            response.read()
    except HTTPError as error:
        assert error.code == 307
        rejected = True
    server.shutdown()
    assert server.escaped == 0, f'redirect target received {server.escaped} authenticated requests'
    assert rejected, 'launcher must reject the original redirect response'
print('PASS: launcher accepts direct responses and rejects redirects before token forwarding')
