__author__ = 'Alex'

import os.path
import json
import pymysql


def upload_tariff(conn, db):
    if os.path.isfile('Tariff.json'):
        with open('Tariff.json') as data_file:
            try:
                tariff = json.load(data_file)
                tariff = tariff['results']
            except ValueError:
                print("Damaged Tariff.json")
                return 0
    else:
        return 0

    a = list(tariff[0].keys())

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'Tariff'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
        cur.execute("CREATE TABLE `tariff` (`id_id` TEXT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
        conn.commit()

    for k in range(0,len(a)):
        print("ALTER TABLE `tariff` ADD COLUMN (%s) TEXT NULL", (a[k]))
        cur.execute("ALTER TABLE `tariff` ADD COLUMN" + str(a[k]) + "TEXT NULL")
        conn.commit()
    cur.close()


    print("Tariff table has been uploaded")
    return 0

def main():

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

            conn_d = pymysql.connect(host=HOST, port=PORT, user=USER, passwd=PASSWD, db=DB, charset=CHARSET)

            upload_tariff(conn_d, DB)


            conn_d.close()
        except Exception as exc:
            print(exc.args[1])

    else:
        print("No database_host file")
        return 1
    return 0

main()

