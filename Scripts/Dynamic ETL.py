__author__ = 'Alex'
import os.path
import json
import pymysql

# Чтение файла конфигураций и подключение к базе
try:
    while 1:
        if os.path.isfile('database_host.inf'):
            with open('database_host.inf') as data_file:
                database_host = data_file.read().splitlines()
                database_host = database_host[1].split()

                try:
                    HOST = database_host[0]
                    PORT = int(database_host[1])
                    USER = database_host[2]
                    DB = database_host[3]
                    CHARSET = database_host[4]
                    print("Enter password for login: ", USER)
                    PASSWD = input()

                    conn = pymysql.connect(host=HOST, port=PORT, user=USER, passwd=PASSWD, db=DB, charset=CHARSET)

                except Exception as exc:
                    print(exc.args[1])
                    continue

    # Считывание списка json файлов
        try:
            print("Enter json directory")
            dir = input()
            if dir[:1] != '\\':
                dir = dir + '\\'

            files = os.listdir(dir)
            if not files:
                print("there is no files")
                continue
            jsons = list(filter(lambda x: x.endswith('.json'), files))
            if not jsons:
                print("there is no jsons")
                continue
        except Exception as exc:
            print(exc.args)
            continue

        for k in range(0, len(jsons)):
            with open(dir + jsons[k], encoding='utf-8') as data_file:
                try:
                    entries = json.load(data_file)
                    entries = entries['results']
                except ValueError:
                    print("DAMAGED", jsons[k])
                    continue
            name = jsons[k][:-5]

    # Динамическое создание таблиц
            cur = conn.cursor()
            cur.execute("SHOW TABLES LIKE '%s'" % name)
            result = cur.fetchone()
            if result:
                abv = 1
            else:
                cur.execute("CREATE TABLE `%s` (`objectId` varchar(40) NOT NULL, PRIMARY KEY (`objectId`))" % name)
                conn.commit()

            keys = list(entries[0])

            for j in range(0, len(keys)):

                query = "SHOW COLUMNS FROM `%s` LIKE '%s'" % (name, keys[j])
                cur.execute(query)
                result = cur.fetchone()
                if result:
                    abv = 1
                else:
                    if (keys[j] == 'createdAt') or (keys[j] == 'updatedAt') or ('Time' in keys[j]):
                        query = "ALTER TABLE `%s` ADD `%s` datetime" % (name, keys[j])
                    else:
                        query = "ALTER TABLE `%s` ADD `%s` text" % (name, keys[j])
                    cur.execute(query)
                    conn.commit()

    # Вставка данных в таблицу и много магии с приведением типов и форматов
            keys = list(entries[0])
            values = ''
            for j in range(0, len(entries)):
                for i in range(0, len(keys)):
                    string = entries[j].get(keys[i])
                    if (keys[i] == 'createdAt') or (keys[i] == 'updatedAt'):
                        string = string.replace('T', ' ')[:-5]

                    if isinstance(string, dict):
                        if string.get('__type') == 'Pointer':
                            string = string.get('objectId')
                        else:
                            if string.get('__type') == 'Date':
                                string = string.get('iso').replace('T', ' ')[:-5]
                            else:
                                if string.get('__type') == 'GeoPoint':
                                    string = str(string.get('longitude')) + ' ' + str(string.pop('latitude'))

                    values = values + ', ' + '\'' + str(string).replace('\'', r'\'') + '\''
                values = values[2:]
                columns = str(keys)[1:-1].replace('\'', '`')

                query = "INSERT IGNORE INTO `%s` (%s) VALUES (%s)" % (name, columns, values)
                try:
                    cur.execute(query)
                    conn.commit()
                except Exception as exc:
                    print(exc.args)
                values = ''
            print("SUCCESSFULL uploaded of %s" % name)
        break
except Exception as exc:
    print(exc.args)





