"""
Servidor HTTP local para el dashboard web de Hidrandina.
Sirve serving_layer/ en http://localhost:8050/dashboard.html
"""
import http.server
import os
import socketserver
import sys
import webbrowser

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SERVING_PATH = os.environ.get(
    "RUTA_SERVING",
    os.path.join(PROJECT_ROOT, "serving_layer"),
)
PORT = int(os.environ.get("DASHBOARD_PORT", "8050"))


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

    def server_bind(self):
        self.socket.setsockopt(socketserver.socket.SOL_SOCKET, socketserver.socket.SO_REUSEADDR, 1)
        super().server_bind()


def main():
    if not os.path.isfile(os.path.join(SERVING_PATH, "dashboard.html")):
        print(f"ERROR: No se encontro dashboard.html en {SERVING_PATH}")
        print("Ejecute primero: python serving_layer/serving.py --export-only")
        sys.exit(1)

    os.chdir(SERVING_PATH)
    url = f"http://localhost:{PORT}/dashboard.html"
    print(f"Dashboard en: {url}")
    print("Ctrl+C para detener")

    try:
        webbrowser.open(url)
    except Exception:
        pass

    with ReusableTCPServer(("", PORT), http.server.SimpleHTTPRequestHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServidor detenido.")


if __name__ == "__main__":
    main()
