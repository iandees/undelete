"""Local dev server that serves both the web UI and data files with CORS and byte range support."""

import http.server
import mimetypes
import os
import sys
from pathlib import Path

# Register parquet MIME type
mimetypes.add_type("application/octet-stream", ".parquet")


class CORSRangeHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Range")
        self.send_header("Access-Control-Expose-Headers", "Content-Range, Content-Length")
        self.send_header("Accept-Ranges", "bytes")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        range_header = self.headers.get("Range")
        if not range_header:
            return super().do_GET()

        path = self.translate_path(self.path)
        try:
            file_size = os.path.getsize(path)
        except OSError:
            self.send_error(404)
            return

        # Parse "bytes=start-end"
        range_spec = range_header.replace("bytes=", "")
        parts = range_spec.split("-")
        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if parts[1] else file_size - 1
        end = min(end, file_size - 1)
        length = end - start + 1

        self.send_response(206)
        self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
        self.send_header("Content-Length", str(length))
        self.send_header("Content-Type", self.guess_type(path))
        self.end_headers()

        with open(path, "rb") as f:
            f.seek(start)
            self.wfile.write(f.read(length))


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    os.chdir(Path(__file__).parent)
    server = http.server.HTTPServer(("", port), CORSRangeHandler)
    print(f"Serving on http://localhost:{port}")
    print(f"  UI:    http://localhost:{port}/web/")
    print(f"  Data:  http://localhost:{port}/data/parquet/")
    server.serve_forever()
