# Корпус оригинальных текстов о любви
Это репозиторий корпуса оригинальных текстов о любви (КОРГИ), собранного из текстов песен, цитат, признаний и книг о любви. Объем корпуса -- 333976 слов, 43650 предложений. 


Сайт корпуса -- https://lovecorp22.pythonanywhere.com/

## О том, откуда брались данные
Данные для корпуса были собраны с помощью парсинга с [сайта о любви](https://skynight.ru/), дизайном которого мы вдохновлялись при создании веб-интерфейса корпуса. Кроме того, в коллекцию вошли три признанных шедевра современной литературы -- романы "После" Анны Тодд, "Сумерки" Стефани Майерс и "P.S. Я все еще люблю тебя" Дженни Хан. Скрипт для парсинга и сайта и текстов книг находится в тетрадке ``GettingData.ipynb``


## О том, как размечался корпус
Для разметки корпуса был использован морфологический анализатор spacy и стандарт Universal Dependencies [для русского](https://github.com/olesar/ruUD/blob/master/conversion/RussianUD_XPOSlist.md). Единственный тег, который мы исключили и заменили на VERB -- AUX. Тексты были очищены от слипшихся знаков препинания и заглавных букв, разделены на предложения, токенизированы, затем записаны в базу данных (LOVECORP.db). База состоит из трех таблиц -- токены, предложения, тексты, для каждого слова в базе хранится информация о том, из какого оно взято текста и предложения, а также его индекс. Тексты представлены с метаинформацией (тема, источник). Скрипт для разметки и записи в базу данных находится в тетрадке ``Tagging.ipynb``

## О том, как реализован поиск
Алгоритм поиска и комментарии к нему находится в файле ``search.py``. Если кратко -- это алгоритм перебора элементов нескольких массивов, который вычисляет расстояние между словами (индексами) и возвращает результат, если это расстояние равно 1.

## Об использованных библиотеках
* Парсинг: ``requests``, ``pandas``, ``csv``
* БД: ``sqlite3``, ``csv``
* Морфология: ``spacy``, ``nltk``
* Деплой: ``flask``
* Поиск: ``sqlite3``

## Создательницы корпуса
Дунаева Ксения, БКЛ-202 -- разметка корпуса, создание базы данных и алгоритма поиска


Смирнова Варвара, БКЛ-201 -- парсинг, создание сайта и подключение к нему базы данных


Фадеева Полина, БКЛ-202 -- парсинг, подготовка презентации
