__author__ = 'Alex'

import os.path
import json
import pymysql


def upload_tariff(conn, db):
    if os.path.isfile('Tariff.json'):
        with open('Tariff.json') as data_file:
            try:
                tariff = json.load(data_file)
            except ValueError:
                print("Damaged Tariff.json")
                return 0
    else:
        return 0


    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'tariff'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
        cur.execute("CREATE TABLE `tariff` (\
        `tariff_id` varchar(50) NOT NULL,\
        `name` varchar(50) NOT NULL,\
        `description` varchar(50) DEFAULT NULL,\
        `car_class_id` varchar(50) DEFAULT NULL,\
        `included_wait_mins` tinyint(4) DEFAULT NULL,\
        `price_fixed` smallint(6) DEFAULT NULL,\
        `price_per_min` tinyint(4) DEFAULT NULL\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

        conn.commit()

    for k in range(0, len(tariff['results'])):
        s = tariff['results'][k]
        cur.execute("INSERT IGNORE INTO " + db + ".`tariff` (`tariff_id`, `name`, `description`, `car_class_id`, \
        `included_wait_mins`, `price_fixed`, `price_per_min`) VALUES (%s, %s, %s, %s, %s, %s, %s);", (s['objectId'],
        s['name'], s['description'], s['carClass']['objectId'], s['includedWaitMins'], s['priceFixed'], s['pricePerMin']))
    conn.commit()

    cur.close()

    print("Tariff table has been uploaded")
    return 0

def upload_class_id(conn, db):
    if os.path.isfile('CarClass.json'):
        with open('CarClass.json') as data_file:
            try:
                car_class = json.load(data_file)
            except ValueError:
                print("Damaged CarClass.json")
                return 0
    else:
        return 0

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'car_class_id'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
        cur.execute("CREATE TABLE `car_class_id` (\
        `car_class_id` varchar(50) NOT NULL,\
        `name` varchar(50) DEFAULT NULL,\
        `description` varchar(50) DEFAULT NULL,\
        PRIMARY KEY (`car_class_id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
        conn.commit()

    for k in range(0, len(car_class['results'])):
        s = car_class['results'][k]
        cur.execute('INSERT IGNORE INTO ' + db + '.`car_class_id` (`car_class_id`, `name`, `description`) VALUES (%s, %s, %s)',
        ( s['objectId'], s['name'], s['theDescription']))
    conn.commit()

    cur.close()

    print("Class_id table has been uploaded")
    return 0

def upload_car(conn, db):
    if os.path.isfile('Car.json'):
        with open('Car.json') as data_file:
            try:
                car = json.load(data_file)
            except ValueError:
                print("Damaged Car.json")
                return 0
    else:
        return 0

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'car'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
          cur.execute("CREATE TABLE `car` (\
          `car_id` varchar(50) NOT NULL,\
          `taxi_legal_entity_id` varchar(50) NOT NULL,\
          `car_class_id` varchar(50) DEFAULT NULL,\
          `make` varchar(50) DEFAULT NULL,\
          `model` varchar(50) DEFAULT NULL,\
          `color` varchar(50) DEFAULT NULL,\
          `license_plates` varchar(50) DEFAULT NULL,\
          PRIMARY KEY (`car_id`),\
          KEY `taxi_legal_entity_id_idx` (`taxi_legal_entity_id`)\
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
          conn.commit()

    for k in range(0, len(car['results'])):
        s = car['results'][k]
        cur.execute("INSERT IGNORE INTO " + db + ".`car` (`car_id`, `taxi_legal_entity_id`, `car_class_id`, `make`, `model`, \
        `color`, `license_plates`) VALUES (%s, %s, %s, %s, %s, %s, %s);", (s['objectId'], s['taxiLegalEntity']['objectId'],
        s['carClass']['objectId'], s['make'], s['model'], s['color'], s['licensePlates']))
    conn.commit()

    cur.close()

    print("Car table has been uploaded")
    return 0

def upload_taxi_legal_entity(conn, db):
    if os.path.isfile('TaxiLegalEntity.json'):
        with open('TaxiLegalEntity.json') as data_file:
            try:
                taxi_legal_entity = json.load(data_file)
            except ValueError:
                print("Damaged TaxiLegalEntity.json")
                return 0
    else:
        return 0


    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'taxi_legal_entity'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
          cur.execute("CREATE TABLE `taxi_legal_entity` (\
          `taxi_legal_entity_id` varchar(50) NOT NULL,\
          `name` varchar(50) DEFAULT NULL,\
          PRIMARY KEY (`taxi_legal_entity_id`)\
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
          conn.commit()

    for k in range(0, len(taxi_legal_entity['results'])):
        s = taxi_legal_entity['results'][k]
        cur.execute("INSERT IGNORE INTO " + db + ".`taxi_legal_entity` (`taxi_legal_entity_id`, `name`) VALUES (%s, %s);",
        (s['objectId'], s['name']))
    conn.commit()

    cur.close()

    print("TaxiLegalEntity table has been uploaded")
    return 0

def upload_car_driver_availability(conn, db):
    if os.path.isfile('CarDriverAvailability.json'):
        with open('CarDriverAvailability.json') as data_file:
            try:
                car_driver_availability = json.load(data_file)
            except ValueError:
                print("Damaged CarDriverAvailability.json")
                return 0
    else:
        return 0

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'car_driver_availability'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
          cur.execute("CREATE TABLE `car_driver_availability` (\
          `car_id` varchar(50) NOT NULL,\
          `user_id` varchar(50) DEFAULT NULL,\
          PRIMARY KEY (`car_id`)\
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
          conn.commit()

    for k in range(0, len(car_driver_availability['results'])):
        s = car_driver_availability['results'][k]
        cur.execute("INSERT IGNORE INTO " + db + ".`car_driver_availability` (`car_id`, `user_id`) VALUES (%s, %s);",
        (s['car']['objectId'], s['user']['objectId']))
    conn.commit()

    cur.close()

    print("CarDriverAvailability table has been uploaded")

    return 0

def upload_tariff_membership_availability(conn, db):
    if os.path.isfile('TariffMembershipAvailability.json'):
        with open('TariffMembershipAvailability.json') as data_file:
            try:
                tariff_membership_availability = json.load(data_file)
            except ValueError:
                print("Damaged TariffMembershipAvailability.json")
                return 0
    else:
        return 0

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'tariff_membership_availability'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
          cur.execute("CREATE TABLE `tariff_membership_availability` (\
          `available_tariff_id` varchar(50),\
          `user_corporation_membership_id` varchar(50) DEFAULT NULL,\
          PRIMARY KEY (`available_tariff_id`)\
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
          conn.commit()

    for k in range(0, len(tariff_membership_availability['results'])):
        s = tariff_membership_availability['results'][k]
        cur.execute("INSERT IGNORE INTO " + db + ".`tariff_membership_availability` (`available_tariff_id`, \
        `user_corporation_membership_id`) VALUES (%s, %s)", (s['tariff']['objectId'], s['membership']['objectId']))
    conn.commit()

    cur.close()

    print("TariffMembershipAvailability table has been uploaded")
    return 0
# Be careful with DataTime
def upload_user_corporation_membership(conn, db):
    if os.path.isfile('UserCorporationMembership.json'):
        with open('UserCorporationMembership.json') as data_file:
            try:
                user_corporation_membership = json.load(data_file)
            except ValueError:
                print("Damaged UserCorporationMembership.json")
                return 0
    else:
        return 0

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'user_corporation_membership'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
        cur.execute("CREATE TABLE `user_corporation_membership` (\
        `user_id` varchar(50) DEFAULT NULL,\
        `corporation_id` varchar(50) DEFAULT NULL,\
        `starts_at` datetime DEFAULT NULL,\
        `ends_at` datetime DEFAULT NULL\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
        conn.commit()

    for k in range(0, len(user_corporation_membership['results'])):
        s = user_corporation_membership['results'][k]

        s['startTime']['new'] = s['startTime']['iso'].replace('T', ' ')
        s['startTime']['new'] = s['startTime']['new'].replace('.000Z', '')

        if s.get('endTime'):
            s['endTime']['new'] = s['endTime']['iso'].replace('T', ' ')
            endtime = s['endTime']['new'].replace('.000Z', '')
        else:
            endtime = s.get('endTime')

        cur.execute("INSERT IGNORE INTO cosmos.user_corporation_membership (user_id, corporation_id, starts_at, ends_at)\
        VALUES (%s, %s, %s, %s)", (s['user']['objectId'], s['corporation']['objectId'], s['startTime']['new'], endtime))
        conn.commit()

    cur.close()

    print("User_corporation_membership table has been uploaded")
    return 0

def upload_corporation(conn, db):
    if os.path.isfile('Corporation.json'):
        with open('Corporation.json') as data_file:
            try:
                corporation = json.load(data_file)
            except ValueError:
                print("Damaged Corporation.json")
                return 0
    else:
        return 0

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'corporation'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
        cur.execute("CREATE TABLE `corporation` (\
        `corporation_id` varchar(50) DEFAULT NULL,\
        `name` varchar(50) DEFAULT NULL\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
        conn.commit()

    for k in range(0, len(corporation['results'])):
        s = corporation['results'][k]
        cur.execute("INSERT IGNORE INTO " + db + ".`corporation` (`corporation_id`, `name`) VALUES (%s, %s)", (s['objectId'],
                                                                                                      s['name']))
    conn.commit()

    cur.close()

    print("Corporation table has been uploaded")
    return 0

def upload_user(conn, db):

    if os.path.isfile('_User.json'):
        with open('_User.json') as data_file:
            try:
                user = json.load(data_file)
            except ValueError:
                print("Damaged _User.json")
                return 0


    else:
        return 0

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'user'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
        cur.execute("CREATE TABLE `user` (\
        `user_id` varchar(50) DEFAULT NULL,\
        `name` varchar(50) DEFAULT NULL,\
        `phone` varchar(50) DEFAULT NULL,\
        `email` varchar(50) DEFAULT NULL,\
        `is_driver` bit(1) DEFAULT NULL\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
        conn.commit()

    for k in range(0, len(user['results'])):

        s = user['results'][k]
        cur.execute("INSERT IGNORE INTO cosmos.user (`user_id`, `name`, `phone`, `email`, `is_driver`)\
        VALUES (%s, %s, %s, %s, %s)", (s['objectId'], s['name'], s['phone'], s['email'], s['isDriver']))
    conn.commit()

    cur.close()

    print("User table has been uploaded")
    return 0

def upload_location(conn, db):
    if os.path.isfile('Location.json'):
        with open('Location.json', encoding='utf-8') as data_file:
            try:
                location = json.load(data_file)
            except ValueError:
                print("Damaged Location.json")
                return 0
    else:
        return 0
    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'location'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
        cur.execute("CREATE TABLE `location` (\
        `location_id` varchar(50) NOT NULL,\
        `longtitude` decimal(20,7) DEFAULT NULL,\
        `latitude` decimal(20,7) DEFAULT NULL,\
        `name` varchar(100) DEFAULT NULL,\
        `city` varchar(50) DEFAULT NULL,\
        `street` varchar(50) DEFAULT NULL,\
        `number` varchar(50) DEFAULT NULL,\
        PRIMARY KEY (`location_id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
        conn.commit()

    for k in range(0, len(location['results'])):

        s = location['results'][k]

        cur.execute("INSERT IGNORE INTO " + db + ".`location` (`location_id`, `longtitude`, `latitude`, `name`, `city`, \
        `street`, `number`)  VALUES (%s, %s, %s, %s, %s, %s, %s);", (s['objectId'],
                                                                    s['coord']['latitude'],
                                                                    s['coord']['longitude'],
                                                                    s.get('name'), s['city'],
                    s['street'], s['number']))
    conn.commit()

    cur.close()

    print("Location table has been uploaded")
    return 0

def upload_driver_duty_session(conn, db):
    if os.path.isfile('DriverDutySession.json'):
        with open('DriverDutySession.json') as data_file:
            try:
                driver_duty_session = json.load(data_file)
            except ValueError:
                print("Damaged DriverDutySession.json")
                return 0
    else:
        return 0

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'driver_duty_session'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
        cur.execute("CREATE TABLE `driver_duty_session` (\
        `driver_duty_session_id` varchar(50) NOT NULL,\
        `driver_user_id` varchar(50) DEFAULT NULL,\
        `car_id` varchar(50) DEFAULT NULL,\
        `started_at` datetime NOT NULL,\
        `finished_at` datetime DEFAULT NULL,\
        `start_location_id` varchar(50) DEFAULT NULL,\
        `end_location_id` varchar(50) DEFAULT NULL,\
        PRIMARY KEY (`driver_duty_session_id`),\
        UNIQUE KEY `driver_duty_session_id_UNIQUE` (`driver_duty_session_id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
        conn.commit()

    for k in range(0, len(driver_duty_session['results'])):
        s = driver_duty_session['results'][k]

        s['startTime']['new'] = s['startTime']['iso'].replace('T', ' ')
        s['startTime']['new'] = s['startTime']['new'].replace('.000Z', '')

        if s.get('finishTime'):
            s['finishTime']['new'] = s['finishTime']['iso'].replace('T', ' ')
            finishtime = s['finishTime']['new'].replace('.000Z', '')
        else:
            finishtime = None

        cur.execute("INSERT IGNORE INTO " + db + ".`driver_duty_session` (`driver_duty_session_id`, \
        `driver_user_id`, `car_id`, `started_at` , `finished_at`, `start_location_id`, `end_location_id`) \
        VALUES (%s, %s, %s, %s, %s, %s, %s)", (s['objectId'], s['driver']['objectId'], s['car']['objectId'],
                                               s['startTime']['new'], finishtime,
                                               s['startLocation']['objectId'], s['endLocation']['objectId']))

    conn.commit()

    cur.close()

    print("Driver_duty_session table has been uploaded")
    return 0

def upload_order(conn, db):
    if os.path.isfile('DriverDutySession.json'):
        with open('Order.json') as data_file:
            try:
                order = json.load(data_file)
            except ValueError:
                print("Damaged Order.json")
                return 0

    else:
        return 0

    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE 'order'")
    result = cur.fetchone()
    if result:
        abv = 1
    else:
        cur.execute("CREATE TABLE `order` (\
        `order_id` varchar(50) DEFAULT NULL,\
        `user_id` varchar(50) DEFAULT NULL,\
        `requested_pickup_time` datetime DEFAULT NULL,\
        `requested_pickup_location_id` varchar(50) DEFAULT NULL,\
        `requested_destination_location_id` varchar(50) DEFAULT NULL,\
        `requested_tariff_id` varchar(50) DEFAULT NULL,\
        `request_submission_time` datetime DEFAULT NULL,\
        `driver_duty_session_id` varchar(50) DEFAULT NULL,\
        `pickup_time` datetime DEFAULT NULL,\
        `start_time` datetime DEFAULT NULL,\
        `finish_time` datetime DEFAULT NULL,\
        `is_corporate` tinyint(1) DEFAULT NULL,\
        `is_cancelled` tinyint(1) DEFAULT NULL,\
         PRIMARY KEY (`order_id`)\
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
        conn.commit()

    for k in range(0, len(order['results'])):
        s = order['results'][k]

        s['startTime']['new'] = s['startTime']['iso'].replace('T', ' ')
        s['startTime']['new'] = s['startTime']['new'].replace('.000Z', '')
        s['pickupTime']['new'] = s['pickupTime']['iso'].replace('T', ' ')
        s['pickupTime']['new'] = s['pickupTime']['new'].replace('.000Z', '')
        s['requestSubmissionTime']['new'] = s['requestSubmissionTime']['iso'].replace('T', ' ')
        s['requestSubmissionTime']['new'] = s['requestSubmissionTime']['new'].replace('.000Z', '')

        if s.get('finishTime'):
            s['finishTime']['new'] = s['finishTime']['iso'].replace('T', ' ')
            finishTime = s['finishTime']['new'].replace('.000Z', '')
        else:
            finishTime = None

        cur.execute("INSERT IGNORE INTO " + db + ".`order` (`order_id`, `user_id`, `requested_pickup_time`, \
        `requested_pickup_location_id`, `requested_destination_location_id`, \
        `request_submission_time`,`driver_duty_session_id`, `pickup_time`, `start_time` , `finish_time`, `is_corporate`, \
        `is_cancelled`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (s['objectId'], s['user']['objectId'], s['pickupTime']['new'],
                     s['requestedPickupLocation']['objectId'], s['requestedDestinationLocation']['objectId'],
                     s['requestSubmissionTime']['new'], s['driverDutySession']['objectId'], s['pickupTime']['new'],
                     s['startTime']['new'], finishTime, s['isCorporate'], s['isCancelled']))

    conn.commit()

    cur.close()

    print("Order table has been uploaded")
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
            upload_class_id(conn_d, DB)
            upload_car(conn_d, DB)
            upload_taxi_legal_entity(conn_d, DB)
            upload_car_driver_availability(conn_d, DB)
            upload_tariff_membership_availability(conn_d, DB)
            upload_user_corporation_membership(conn_d, DB)
            upload_corporation(conn_d, DB)
            upload_user(conn_d, DB)
            upload_location(conn_d, DB)
            upload_driver_duty_session(conn_d, DB)
            upload_order(conn_d, DB)

            conn_d.close()
        except Exception as exc:
            print(exc.args[1])

    else:
        print("No database_host file")
        return 1
    return 0

main()

