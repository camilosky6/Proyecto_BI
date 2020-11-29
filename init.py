#!/usr/bin/python3
# -*- coding: utf-8 -*-

import urllib3
import json
import time
import boto3
#Bucket
bucket_name = 'pruebabucketproyect'

#Conexion a S3
aws_access_key_id = 'AKIAZFC7DVDYSMINXTND'
aws_secret_access_key = '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'
region_name = 'us-east-2'
session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=region_name)
s3 = session.client('s3')


#s3 = boto3.client('s3')

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

#Json con los animes en el top 5000
#5000
topAnime = {'topAnime': []}

#Json con los mangas en el top 5000
#5000
topManga = {'topManga': []}


#Este ciclo trae todos los animes de fantasia
for i in range(1, 33):
    r = http.request('GET', f'https://api.jikan.moe/v3/genre/anime/10/{i}')
    r2 = json.loads(r.data.decode('utf-8'))
    animeFantasy['animeFantasy'] = animeFantasy['animeFantasy'] + r2['anime']
    time.sleep(2)

with open('animeFantasy.json', 'w') as file:
    json.dump(animeFantasy, file, indent=4)

s3.upload_file('animeFantasy.json', bucket_name, 'Anime/Fantasy/animeFantasy.json')



# Este bucle recorre todas las paginas de animes de accion
for i in range(1, 39):
    r = http.request('GET', f'https://api.jikan.moe/v3/genre/anime/1/{i}')
    r2 = json.loads(r.data.decode('utf-8'))
    animeAction['animeAction'] = animeAction['animeAction'] + r2['anime']
    time.sleep(2)

with open('animeAction.json', 'w') as file:
    json.dump(animeAction, file, indent=4)

s3.upload_file('animeAction.json', bucket_name, 'Anime/Action/animeAction.json')

# Este bucle recorre todas las paginas de mangas de acción
for i in range(1, 73):
    r = http.request('GET', f'https://api.jikan.moe/v3/genre/manga/1/{i}')
    r2 = json.loads(r.data.decode('utf-8'))
    mangaAction['mangaAction'] = mangaAction['mangaAction'] + r2['manga']
    time.sleep(2)

with open('mangaAction.json', 'w') as file:
    json.dump(mangaAction, file, indent=4)

s3.upload_file('mangaAction.json', bucket_name, 'Manga/Action/mangaAction.json')


# Este bucle recorre el top 5000 de mangas
for i in range(1, 101):
    r = http.request('GET', f'https://api.jikan.moe/v3/top/manga/{i}/')
    r2 = json.loads(r.data.decode('utf-8'))
    topManga['topManga'] = topManga['topManga'] + r2['top']
    time.sleep(3)

with open('topManga.json', 'w') as file:
    json.dump(topManga, file, indent=4)

s3.upload_file('topManga.json', bucket_name, 'Manga/Top/topManga.json')


# Este bucle recorre el top 5000 de animes
for i in range(1, 101):
    r = http.request('GET', f'https://api.jikan.moe/v3/top/anime/{i}/')
    r2 = json.loads(r.data.decode('utf-8'))
    topAnime['topAnime'] = topAnime['topAnime'] + r2['top']
    time.sleep(3)

with open('topAnime.json', 'w') as file:
    json.dump(topAnime, file, indent=4)

s3.upload_file('topAnime.json', bucket_name, 'Anime/Top/topAnime.json')



# Este bucle recorre todas las paginas de mangas de comedia
for i in range(1, 112):
    r = http.request('GET', f'https://api.jikan.moe/v3/genre/manga/4/{i}')
    r2 = json.loads(r.data.decode('utf-8'))
    mangaComedy['mangaComedy'] = mangaComedy['mangaComedy'] + r2['manga']
    time.sleep(2)

with open('mangaComedy.json', 'w') as file:
    json.dump(mangaComedy, file, indent=4)

s3.upload_file('mangaComedy.json', bucket_name, 'Manga/Comedy/mangaComedy.json')


print(len(animeFantasy['animeFantasy']))

#print(animeAction)
#animes['anime']=animes['anime']+r4['anime']
#print(r.data)
#print(r3)
#print(len(animesAction['animeAction']))
