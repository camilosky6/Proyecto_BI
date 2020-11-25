from jikanpy import Jikan
import urllib3
import json
import time

http = urllib3.PoolManager()

#Json con los animes de accion
#3800
animeAction = {'animeAction': []}

#Json con los animes de fantasia
#3200
animeFantasy = {'animeFantasy': []}

#Json con los mangas de acción
#7200
mangaAction = {'mangaAction': []}

#Json con los mangas de comedia
#11100
mangaComedy = {'mangaComedy': []}

#Json con los animes y mangas proximos a estrenar
seasonLater = {'seasonLater': []}

r = http.request('GET', f'https://api.jikan.moe/v3/genre/manga/4/{i}')
r2 = json.loads(r.data.decode('utf-8'))
print(len(r2))

'''
# Este bucle recorre los animes y mangas proximos a estrenar
for i in range(1, 2):
    r = http.request('GET', f'https://api.jikan.moe/v3/genre/manga/4/{i}')
    r2 = json.loads(r.data.decode('utf-8'))
    mangaComedy['mangaComedy'] = mangaComedy['mangaComedy'] + r2['manga']
    time.sleep(2)
'''



'''
# Este bucle recorre todas las paginas de mangas de comedia
for i in range(1, 112):
    r = http.request('GET', f'https://api.jikan.moe/v3/genre/manga/4/{i}')
    r2 = json.loads(r.data.decode('utf-8'))
    mangaComedy['mangaComedy'] = mangaComedy['mangaComedy'] + r2['manga']
    time.sleep(2)
'''

'''
# Este bucle recorre todas las paginas de mangas de acción
for i in range(1, 73):
    r = http.request('GET', f'https://api.jikan.moe/v3/genre/manga/1/{i}')
    r2 = json.loads(r.data.decode('utf-8'))
    mangaAction['mangaAction'] = mangaAction['mangaAction'] + r2['manga']
    time.sleep(2)
'''


'''
#Este ciclo trae todos los animes de fantasia
for i in range(1, 33):
    r = http.request('GET', f'https://api.jikan.moe/v3/genre/anime/10/{i}')
    r2 = json.loads(r.data.decode('utf-8'))
    animeFantasy['animeFantasy'] = animeFantasy['animeFantasy'] + r2['anime']
    time.sleep(2)
'''

'''
# Este bucle recorre todas las paginas de animes de accion
for i in range(1, 39):
    r = http.request('GET', f'https://api.jikan.moe/v3/genre/anime/1/{i}')
    r2 = json.loads(r.data.decode('utf-8'))
    animeAction['animeAction'] = animeAction['animeAction'] + r2['anime']
    time.sleep(2)
'''


print(len(mangaComedy['mangaComedy']))
#print(animeAction)
#animes['anime']=animes['anime']+r4['anime']
#print(r.data)
#print(r3)
#print(len(animesAction['animeAction']))
