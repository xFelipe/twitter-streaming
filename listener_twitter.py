import socket
from os import environ as env
from dotenv import load_dotenv
from tweepy import StreamingClient, Tweet, StreamRule

load_dotenv()

HOST = env['HOST']
PORT = int(env['PORT'])
AUTH_TOKEN = env['AUTH_TOKEN']
KEYWORD = 'Lula'



s = socket.socket()
s.bind((HOST, PORT))
print(f'Aguardando conexão na porta: {PORT}.')

s.listen(5)
conn, addr = s.accept()
print(f'Recebendo solicitação de {addr}.')

token = ''


class GetTweets(StreamingClient):
    def on_tweet(self, tweet: Tweet):
        print(tweet.text)
        print('='*50)
        conn.send(tweet.text.encode('utf-8', 'ignore'))

printer = GetTweets(AUTH_TOKEN)
printer.add_rules(StreamRule(KEYWORD))
printer.filter()

conn.close()
