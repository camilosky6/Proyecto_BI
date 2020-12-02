import boto3
import pandas as pd
import json
import datetime
import psycopg2

from sqlalchemy import create_engine

# Nombre del bucket de donde se traeran los datos
bucket_name = 'myawsbucketbi'

# Conexion a S3
aws_access_key_id = 'AKIAZFC7DVDYSMINXTND'
aws_secret_access_key = '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'
region_name = 'us-east-2'
session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=region_name)
s3 = session.resource('s3')

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

engine = create_engine('postgresql://postgres:12345678@database-1.cjqx4noi3om2.us-east-2.rds.amazonaws.com:5432/database-1')
data_df_animeAction.to_sql('AnimeAction', con=engine, index=True)
