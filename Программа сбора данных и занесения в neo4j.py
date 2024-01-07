import requests
import pandas as pd
from py2neo import Graph, Node, Relationship
def fetch_movie_data(start_FilmId, end_FildId):
    # Задаем нужные url и доступ к API
    url1 = 'https://kinopoiskapiunofficial.tech/api/v2.2/films/' # URL для получения данных о фильме
    url2 = 'https://kinopoiskapiunofficial.tech/api/v1/staff?filmId=' # URL для получения данных об актерах фильма
    headers = {
    'X-API-KEY': '22deee09-c581-4c94-ab8e-2ad3602a5679',
    'Content-Type': 'application/json'
    }
    final_dataframe = pd.DataFrame()
    # Запускаем цикл сбора данных с заданных FilmId
    for i in range(start_FilmId, end_FildId):
        response1 = requests.get(url=f'{url1}{i}', headers=headers)
        response2 = requests.get(url=f'{url2}{i}', headers=headers)
        # Проверяем существует ли такая страница и начинаем преобразовывать данные
        if response1.status_code == 200 and response2.status_code == 200:
            # Собираем данные о фильме
            json_response1 = response1.json()
            data1 = pd.DataFrame([json_response1])
            data1_v = data1[['nameRu', 'year', 'slogan']]
            # Собираем данные об участниках фильма
            json_response2 = response2.json()
            data2 = pd.DataFrame(json_response2)
            df3 = data2[['nameRu', 'professionKey']]
            new_df = df3.groupby('professionKey').agg({'nameRu': list}).reset_index()
            new_df4 = new_df.set_index('professionKey')
            new_df5 = new_df4.transpose()
            new_df6 = new_df5.reset_index(drop=True)
            # Уберем пустые ('') строки, чтобы не создавать лишних пустых узлов в neo4j
            new_df6['ACTOR'] = new_df6['ACTOR'].apply(lambda x: [item for item in x if item != ''])
            new_df6['DIRECTOR'] = new_df6['DIRECTOR'].apply(lambda x: [item for item in x if item != ''])
            new_df6['PRODUCER'] = new_df6['PRODUCER'].apply(lambda x: [item for item in x if item != ''])
            new_df6['WRITER'] = new_df6['WRITER'].apply(lambda x: [item for item in x if item != ''])
            combined_df = pd.concat([data1_v, new_df6], axis=1)
            # Соединяем две таблицы
            final_dataframe = pd.concat([final_dataframe, combined_df], ignore_index=True)
        else:
            print(f"Ошибка получения данных для фильма с ID {i}: {response1.status_code}")
    return final_dataframe
# Задаем нужные id фильмов для сбора информации
start_FilmId = 300
end_FildId = 320
# Сохраняем полученные данные в переменную result_dataframe
result_dataframe = fetch_movie_data(start_FilmId, end_FildId)
# Подключение к базе данных Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))
# Создаем словарь действия в фильме каждого участника
profession_types = {
    'ACTOR' : 'ACTED_IN',
    'DIRECTOR' : 'DIRECTED',
    'PRODUCER' : 'PRODUCED',
    'WRITER' : 'WROTE'
}
# Запускаем цикл создания узлов фильмов и участников в neo4j
for index, record in result_dataframe.iterrows():
    movie_title = record['nameRu']
    # Создание узла для фильма
    movie_node = Node("Movie", title=movie_title, year=record['year'], slogan=record['slogan'])
    graph.merge(movie_node, "Movie", "title")
    # Создание узлов для участников
    for profession in ['ACTOR', 'DIRECTOR', 'PRODUCER', 'WRITER']:
        if profession in record:
            for person_name in record[profession]:
                person_node = Node('Person', name=person_name)
                graph.merge(person_node, 'Person', 'name')
                relationship_type = Relationship(person_node, profession_types[profession], movie_node)
                graph.merge(relationship_type)