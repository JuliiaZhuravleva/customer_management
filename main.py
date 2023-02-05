import configparser
import customer_management as cm

config = configparser.ConfigParser()
config.read("settings.ini")

database = config['database_params']['database']
user = config['database_params']['user']
password = config['database_params']['password']


db = cm.MyDatabase(db=database, user=user, password=password)

db.drop_database_structure()
db.create_database_structure()

db.add_client(
    first_name='Елена',
    last_name='Анисимова',
    email='eanisimova@test.com',
    phones=['555-55-55', '9-888-707-55-66'])
db.add_client(
    first_name='Павел',
    last_name='Потапов',
    email='ppotapov@test.com')
db.add_client(
    first_name='Павел',
    last_name='Анохин',
    email='1@2.com')

client_id = db.get_client_id(email='1@2.com')[0]
db.add_phone(client_id, '222-22-22')
db.change_client_info(client_id, email='one@two.com')

client_id = db.get_client_id(email='ppotapov@test.com')[0]
db.remove_client(client_id)

client_id = db.get_client_id(phone='555-55-55')[0]
db.change_client_info(client_id, email='e_anisimova@test.com')
db.remove_phone('555-55-55')


db.close()

