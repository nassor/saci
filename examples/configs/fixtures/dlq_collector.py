"""The endpoint examples/configs/dlq.kdl POSTs to, once you want it to work.

Start it after watching the letter accumulate: the dead letter queue's
automatic replay delivers everything it is holding within one backoff step,
and every delivery prints one line here.
"""

from http.server import BaseHTTPRequestHandler, HTTPServer


class Collector(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        print(f"POST {self.path}: {len(body)} bytes, {len(body.splitlines())} rows")
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_args: object) -> None:
        pass  # The print above is the whole log.


if __name__ == "__main__":
    print("listening on http://127.0.0.1:18099")
    HTTPServer(("127.0.0.1", 18099), Collector).serve_forever()
