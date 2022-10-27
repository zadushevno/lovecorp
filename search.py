import sqlite3
import re
import pandas as pd
import os.path

import spacy


class DbManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.nlp = spacy.load("ru_core_news_sm")

    def check_query(self, query): #функция для проверки типа запроса
        if not query.startswith(('"', 'A', 'C', 'N', 'D', 'P', 'S', 'V', 'I')) and not '+' in query:
            doc = self.nlp(query)
            lemma = doc[0].lemma_
            return lemma
        else:
            return query

    def search_each(self, query, cur): #функция поиска в базе данных
        checked = self.check_query(query)
        res = ""
        if '+' in checked:
            query_wordwithpos = '''
            SELECT * FROM token
            JOIN texts ON token.text_id = texts.id
            JOIN sentences ON token.sentence_id = sentences.id
            WHERE token = ? and pos = ?
            '''  #запрос для случаев знать+NOUN, первая часть -- конкретная словоформа
            checked = checked.split('+')
            token = checked[0]
            pos = checked[1]
            cur.execute(query_wordwithpos, (token, pos))
            res = cur.fetchall()
        elif checked.startswith('"'):
            checked = checked.strip('"')
            query_exactform = '''
            SELECT * FROM token
            JOIN texts ON token.text_id = texts.id
            JOIN sentences ON token.sentence_id = sentences.id
        WHERE token = ?
        ''' #запрос для поиска по конкретной словоформе
            cur.execute(query_exactform, (checked,)) 
            res = cur.fetchall()
        elif checked.startswith(('A', 'C', 'N', 'D', 'P', 'S', 'V', 'I')):
            query_pos = '''
            SELECT * FROM token
            JOIN texts ON token.text_id = texts.id
            JOIN sentences ON token.sentence_id = sentences.id
        WHERE pos = ?
        ''' #запрос для поиска по части речи
            cur.execute(query_pos, (checked,))
            res = cur.fetchall()
        else:
            query_lemma = '''
            SELECT * FROM token
            JOIN texts ON token.text_id = texts.id
            JOIN sentences ON token.sentence_id = sentences.id
            WHERE lem = ?
            ''' #запрос для поиска по лемме
            cur.execute(query_lemma, (checked,))
            res = cur.fetchall()
        return res

    def search(self, queries):
        result = []
        themes = {'stihi' : 'стихотворение', 'anekdots' : 'анекдот', 'aphorism' : 'афоризм', 'book': 'книга',  
        'pesni' : 'песня', 'priznaniya' : 'признание в любви', 'quote' : 'цитата', 'sms' : 'смс-сообщение любимому',  
        'status' : 'статус'}
        with sqlite3.connect(self.db_path) as con:
            cur = con.cursor()
            queries = queries.split()
            if len(queries) == 1:
                tokens = self.search_each(queries[0], cur)
                for token in tokens:
                    result.append(
                        {
                            'result': token[3],
                            'sentence': token[12],
                            'theme': themes[token[8]],
                            'source': token[9]
                        }
                    )
            elif len(queries) == 2:
                first = self.search_each(queries[0], cur)
                second = self.search_each(queries[1], cur)
                for i in first: #перебираем результаты для двух запросов
                    for j in second:
                        if j[0]-i[0] == 1: #смотрим расстояние между токенами, если 1 -- выдаем результат
                            res = i[3] + ' ' + j[3]
                            result.append(
                                {
                                    'result': res,
                                    'sentence': i[12],
                                    'theme': themes[i[8]],
                                    'source': i[9]
                                }
                            )
            else:
                first = self.search_each(queries[0], cur)
                second = self.search_each(queries[1], cur)
                third = self.search_each(queries[2], cur)
                for i in first:
                    for j in second:
                        if j[0]-i[0] == 1: #прежде чем перебирать результаты третьего запроса, отсекаем случаи, когда между первыми двумя токенами расстояние не 1
                            for z in third:
                                if z[0]-j[0] == 1: #если расстояние между токенами 2 и 3 равно 1, выдаем результат
                                    res = i[3] + ' ' + j[3] + ' ' + z[3]
                                    result.append(
                                        {
                                            'result': res,
                                            'sentence': i[12],
                                            'theme': themes[i[8]],
                                            'source': i[9]
                                        }
                                    )
        return result
