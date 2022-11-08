# serve HTTP page of the index.html inside of /target   for documentation generated by   dbt docs generate
from  http.server import HTTPServer, SimpleHTTPRequestHandler 
import sys, threading, webbrowser
ip = '127.0.0.1'
port = 8080
url = f'http://{ip}:{port}'

def start_server():
    server_address = (ip, port )
    httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)
    print('-'*80)
    print( "CTRL-Click to view:  http://127.0.0.1:8080" )
    print('-'*80)
    print( f'>>> Server started to serve /target/index.html  generated from: dbt docs generate')
    httpd.serve_forever()

threading.Thread(target = start_server).start()
webbrowser.open( url )

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        print(f'<<< server stopped')
        sys.exit(0)
