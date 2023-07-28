# Twitter streaming capture

## Environment Vars:
 - `AUTH_TOKEN`: Twitter Bearer Token
 - `HOST`: Listener host. Usually `localhost`
 - `PORT`: Listener port. Chose a high door, like `9009`

## Run
 - Execute listener: `python3 listener_twitter.py`
 - In another terminal, run the client: `python3 client_csv.py` or `python3 client_console.py`