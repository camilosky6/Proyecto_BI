import boto3
import pandas as pd
import json
import datetime
import psycopg2

from sqlalchemy import create_engine

# Nombre del bucket de donde se traeran los datos
bucket_name = 'mybucketbi'

engine = create_engine(
    'postgresql://postgres:12345678@database-1.cdzpvsiyqdm9.us-east-2.rds.amazonaws.com:5432/postgres')

# Conexion a S3
aws_access_key_id = 'AKIA4SLODYPZZKT2NJUI'
aws_secret_access_key = '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'
region_name = 'us-east-2'
session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=region_name)
s3 = session.resource('s3')

'''
# Traemos el json del top de manga
content_MangaTop = s3.Object(bucket_name, 'Manga/Top/topManga.json')

file_content_MangaTop = content_MangaTop.get()['Body'].read().decode('utf-8')
json_content_file_MangaTop = json.loads(file_content_MangaTop)
mangaTop = json_content_file_MangaTop['topManga']

df_mangaTop = pd.DataFrame.from_dict(mangaTop)

df_mangaTop['origen'] = "https://api.jikan.moe/v3/top/manga/"
fecha = datetime.datetime.now()
df_mangaTop['date_add'] = fecha.timestamp()
df_mangaTop['event'] = "Carga a la base de datos, cloudwatch"

data_df_mangaTop = pd.DataFrame(columns=['mal_id', 'rank', 'title', 'url', 'type', 'volumes', 'start_date', 'end_date',
                                         'members', 'score', 'image_url', 'origen', 'date_add', 'event'])

data_df_mangaTop = data_df_mangaTop.append(df_mangaTop[
                                               ['mal_id', 'rank', 'title', 'url', 'type', 'volumes', 'start_date',
                                                'end_date', 'members', 'score', 'image_url', 'origen', 'date_add'
                                                , 'event']],
                                           ignore_index=True)

data_df_mangaTop.to_sql('MangaTop', con=engine, index=True)



# Traemos el json de Mangas de comedia
content_mangaComedy = s3.Object(bucket_name, 'Manga/Comedy/mangaComedy.json')

file_content_MangaComedy = content_mangaComedy.get()['Body'].read().decode('utf-8')
json_content_file_MangaComedy = json.loads(file_content_MangaComedy)
mangaComedy = json_content_file_MangaComedy['mangaComedy']

df_mangaComedy = pd.DataFrame.from_dict(mangaComedy)

df_mangaComedy['generous'] = ""
df_mangaComedy['autor'] = ""
df_mangaComedy['serial'] = ""
df_mangaComedy['origen'] = "https://api.jikan.moe/v3/genre/manga/7/"
fecha = datetime.datetime.now()
df_mangaComedy['date_add'] = fecha.timestamp()
df_mangaComedy['event'] = "Carga a la base de datos, cloudwatch"

data_df_mangaComedy = pd.DataFrame(columns=['mal_id', 'url', 'title', 'image_url', 'synopsis', 'type',
                                            'publishing_start', 'members', 'generous', 'autor', 'score',
                                            'serial', 'event', 'origen', 'date_add'])

for i in df_mangaComedy.index:
    generos = ""

    for gen in mangaComedy[i]['genres']:
        generos = generos + ", " + gen['name']

    df_mangaComedy.loc[i, 'generous'] = generos

    arr1 = len(mangaComedy[i]['authors'])
    if arr1 > 0:
        productor = mangaComedy[i]['authors'][0]['name']
        df_mangaComedy.loc[i, 'autor'] = productor
    else:
        df_mangaComedy.loc[i, 'autor'] = "N/A"

    arr = len(mangaComedy[i]['serialization'])
    if arr > 0:
        licencia = mangaComedy[i]['serialization'][0]
        df_mangaComedy.loc[i, 'serial'] = licencia
    else:
        df_mangaComedy.loc[i, 'serial'] = "N/A"

data_df_mangaComedy = data_df_mangaComedy.append(df_mangaComedy[
                                                     ['mal_id', 'url', 'title', 'image_url', 'synopsis', 'type',
                                                      'publishing_start', 'members', 'generous', 'autor', 'score',
                                                      'serial', 'event', 'origen', 'date_add']], ignore_index=True)

data_df_mangaComedy.to_sql('MangaComedy', con=engine, index=True)



# Traemos el json de Mangas de accion
content_mangaAction = s3.Object(bucket_name, 'Manga/Action/mangaAction.json')

file_content_MangaAction = content_mangaAction.get()['Body'].read().decode('utf-8')
json_content_file_MangaAction = json.loads(file_content_MangaAction)
mangaAction = json_content_file_MangaAction['mangaAction']

df_mangaAction = pd.DataFrame.from_dict(mangaAction)

df_mangaAction['generous'] = ""
df_mangaAction['autor'] = ""
df_mangaAction['serial'] = ""
df_mangaAction['origen'] = "https://api.jikan.moe/v3/genre/manga/1/"
fecha = datetime.datetime.now()
df_mangaAction['date_add'] = fecha.timestamp()
df_mangaAction['event'] = "Carga a la base de datos, cloudwatch"

data_df_mangaAction = pd.DataFrame(columns=['mal_id', 'url', 'title', 'image_url', 'synopsis', 'type',
                                            'publishing_start', 'members', 'generous', 'autor', 'score',
                                            'serial', 'event', 'origen', 'date_add'])

for i in df_mangaAction.index:
    generos = ""

    for gen in mangaAction[i]['genres']:
        generos = generos + ", " + gen['name']

    df_mangaAction.loc[i, 'generous'] = generos

    arr1 = len(mangaAction[i]['authors'])
    if arr1 > 0:
        productor = mangaAction[i]['authors'][0]['name']
        df_mangaAction.loc[i, 'autor'] = productor
    else:
        df_mangaAction.loc[i, 'autor'] = "N/A"

    arr = len(mangaAction[i]['serialization'])
    if arr > 0:
        licencia = mangaAction[i]['serialization'][0]
        df_mangaAction.loc[i, 'serial'] = licencia
    else:
        df_mangaAction.loc[i, 'serial'] = "N/A"

data_df_mangaAction = data_df_mangaAction.append(df_mangaAction[
                                                     ['mal_id', 'url', 'title', 'image_url', 'synopsis', 'type',
                                                      'publishing_start', 'members', 'generous', 'autor', 'score',
                                                      'serial', 'event', 'origen', 'date_add']], ignore_index=True)

data_df_mangaAction.to_sql('MangaAction', con=engine, index=True)

# Traemos el json del top de anime
content_AnimeTop = s3.Object(bucket_name, 'Anime/Top/topAnime.json')

file_content_AnimeTop = content_AnimeTop.get()['Body'].read().decode('utf-8')
json_content_file_AnimeTop = json.loads(file_content_AnimeTop)
animeTop = json_content_file_AnimeTop['topAnime']

df_animeTop = pd.DataFrame.from_dict(animeTop)

df_animeTop['origen'] = "https://api.jikan.moe/v3/top/anime/"
fecha = datetime.datetime.now()
df_animeTop['date_add'] = fecha.timestamp()
df_animeTop['event'] = "Carga a la base de datos, cloudwatch"

data_df_animeTop = pd.DataFrame(columns=['mal_id', 'rank', 'title', 'url', 'image_url', 'type',
                                         'start_date', 'members', 'score', 'origen', 'date_add', 'event'])

data_df_animeTop = data_df_animeTop.append(df_animeTop[
                                               ['mal_id', 'rank', 'title', 'url', 'image_url', 'type',
                                                'start_date', 'members', 'score', 'origen', 'date_add', 'event']],
                                           ignore_index=True)

data_df_animeTop.to_sql('AnimeTop', con=engine, index=True)


# Traemos el json de animes de fantasia
content_AnimeFantasy = s3.Object(bucket_name, 'Anime/Fantasy/animeFantasy.json')

file_content_AnimeFantasy = content_AnimeFantasy.get()['Body'].read().decode('utf-8')
json_content_file_AnimeFantasy = json.loads(file_content_AnimeFantasy)
animeFantasy = json_content_file_AnimeFantasy['animeFantasy']

df_animeFantasy = pd.DataFrame.from_dict(animeFantasy)

df_animeFantasy['generous'] = ""
df_animeFantasy['production'] = ""
df_animeFantasy['licence'] = ""
df_animeFantasy['origen'] = "https://jikan.docs.apiary.io/#reference/0/genre/genre-request-example+schema?console=1"
fecha = datetime.datetime.now()
df_animeFantasy['date_add'] = fecha.timestamp()
df_animeFantasy['event'] = "Carga a la base de datos, cloudwatch"

data_df_animeFantasy = pd.DataFrame(columns=['mal_id', 'url', 'title', 'image_url', 'synopsis', 'type',
                                             'airing_start',
                                             'episodes', 'generous', 'source', 'production', 'score',
                                             'licence', 'r18',
                                             'kids', 'event', 'origen', 'date_add'])

for i in df_animeFantasy.index:
    generos = ""

    for gen in animeFantasy[i]['genres']:
        generos = generos + ", " + gen['name']

    df_animeFantasy.loc[i, 'generous'] = generos

    arr1 = len(animeFantasy[i]['producers'])
    if arr1 > 0:
        productor = animeFantasy[i]['producers'][0]['name']
        df_animeFantasy.loc[i, 'production'] = productor
    else:
        df_animeFantasy.loc[i, 'production'] = "N/A"

    arr = len(animeFantasy[i]['licensors'])
    if arr > 0:
        licencia = animeFantasy[i]['licensors'][0]
        df_animeFantasy.loc[i, 'licence'] = licencia
    else:
        df_animeFantasy.loc[i, 'licence'] = "N/A"

data_df_animeFantasy = data_df_animeFantasy.append(df_animeFantasy[
                                                     ['mal_id', 'url', 'title', 'image_url', 'synopsis', 'type',
                                                      'airing_start',
                                                      'episodes', 'generous', 'source', 'production', 'score',
                                                      'licence', 'r18',
                                                      'kids', 'event', 'origen', 'date_add']], ignore_index=True)


data_df_animeFantasy.to_sql('AnimeFantasy', con=engine, index=True)
'''

# Traemos el json de animes de accion
content_AnimeAction = s3.Object(bucket_name, 'Anime/Action/animeAction.json')

file_content_AnimeAction = content_AnimeAction.get()['Body'].read().decode('utf-8')
json_content_file_AnimeAction = json.loads(file_content_AnimeAction)
animeAction = json_content_file_AnimeAction['animeAction']

df_animeAction = pd.DataFrame.from_dict(animeAction)

df_animeAction['generous'] = ""
df_animeAction['production'] = ""
df_animeAction['licence'] = ""
df_animeAction['origen'] = "https://jikan.docs.apiary.io/#reference/0/genre/genre-request-example+schema?console=1"
fecha = datetime.datetime.now()
df_animeAction['date_add'] = fecha.timestamp()
df_animeAction['event'] = "Carga a la base de datos, cloudwatch"

data_df_animeAction = pd.DataFrame(columns=['mal_id', 'url', 'title', 'image_url', 'synopsis', 'type',
                                            'airing_start',
                                            'episodes', 'generous', 'source', 'production', 'score',
                                            'licence', 'r18',
                                            'kids', 'event', 'origen', 'date_add'])


for i in df_animeAction.index:
    generos = ""

    for gen in animeAction[i]['genres']:
        generos = generos + ", " + gen['name']

    df_animeAction.loc[i, 'generous'] = generos

    arr1 = len(animeAction[i]['producers'])
    if arr1 > 0:
        productor = animeAction[i]['producers'][0]['name']
        df_animeAction.loc[i, 'production'] = productor
    else:
        df_animeAction.loc[i, 'production'] = "N/A"

    arr = len(animeAction[i]['licensors'])
    if arr > 0:
        licencia = animeAction[i]['licensors'][0]
        df_animeAction.loc[i, 'licence'] = licencia
    else:
        df_animeAction.loc[i, 'licence'] = "N/A"

data_df_animeAction = data_df_animeAction.append(df_animeAction[
                                                     ['mal_id', 'url', 'title', 'image_url', 'synopsis', 'type',
                                                      'airing_start',
                                                      'episodes', 'generous', 'source', 'production', 'score',
                                                      'licence', 'r18',
                                                      'kids', 'event', 'origen', 'date_add']], ignore_index=True)


data_df_animeAction.to_sql('AnimeAction', con=engine, index=True)

