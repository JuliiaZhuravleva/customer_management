import psycopg2
from psycopg2 import sql


class MyDatabase:
    def __init__(self, db, user, password):
        self.conn = psycopg2.connect(database=db, user=user, password=password)
        self.cur = self.conn.cursor()

    def query(self, query, params=None, return_value=False, return_values=False):
        if params:
            self.cur.execute(query, params)
        else:
            self.cur.execute(query)
        if return_value:
            value = self.cur.fetchone()
            return value
        if return_values:
            values = self.cur.fetchall()
            return values

    def close(self):
        self.cur.close()
        self.conn.close()

    def create_database_structure(self):
        sql_table_clients = '''
        CREATE TABLE IF NOT EXISTS clients 
        (
        id          SERIAL PRIMARY   KEY,
        first_name  VARCHAR(100)     NOT NULL,
        last_name   VARCHAR(100)     NOT NULL,
        email       VARCHAR(100)     NOT NULL    UNIQUE     CHECK (email LIKE '_%@_%._%')
        );
        '''

        sql_table_phones = '''
        CREATE TABLE IF NOT EXISTS phones 
        (
        id          SERIAL      PRIMARY KEY,
        client_id   INTEGER     NOT NULL    REFERENCES clients(id),
        phone       VARCHAR(60) NOT NULL    UNIQUE
        );
        '''

        self.query(sql_table_clients)
        self.query(sql_table_phones)
        self.conn.commit()

    def drop_database_structure(self):
        sql = '''
        DROP    TABLE   clients     CASCADE;
        DROP    TABLE   phones;
        '''

        self.query(sql)
        self.conn.commit()

    def add_client(self, first_name, last_name, email, phones=None):
        sql_insert_client = '''
        INSERT INTO clients (first_name, last_name, email) 
        values (%(first_name)s, %(last_name)s, %(email)s)
        RETURNING id;
        '''
        client_info = {
            'first_name': first_name,
            'last_name': last_name,
            'email': email
        }

        client_id = self.query(sql_insert_client, client_info, return_value=True)
        self.conn.commit()

        if phones:
            for phone in phones:
                self.add_phone(client_id, phone)
            self.conn.commit()

        return client_id

    def remove_client(self, client_id):
        sql_delete_phones = '''
        DELETE FROM phones WHERE client_id = %(client_id)s;
        '''
        sql_delete_client = '''
        DELETE FROM clients WHERE id = %(client_id)s;
        '''
        params = {'client_id': client_id}

        self.query(sql_delete_phones, params)
        self.query(sql_delete_client, params)
        self.conn.commit()

    def add_phone(self, client_id, phone):
        sql_insert_phones = '''
                    INSERT INTO phones (client_id, phone)
                    values (%(client_id)s, %(phone)s)
                    '''
        phone_info = {
            'client_id': client_id,
            'phone': phone
        }
        self.query(sql_insert_phones, phone_info)
        self.conn.commit()

    def remove_phone(self, phone):
        sql_delete_phone = '''
        DELETE FROM phones WHERE phone = %(phone)s;
        '''
        params = {'phone': phone}

        self.query(sql_delete_phone, params)
        self.conn.commit()

    def change_client_info(self, client_id, first_name=None, last_name=None, email=None):
        set_list = {}

        if first_name:
            set_list['first_name'] = first_name
        if last_name:
            set_list['last_name'] = last_name
        if email:
            set_list['email'] = email

        sql_update = sql.SQL("""
                UPDATE clients SET {set_list} WHERE id = {client_id};
                """.format(
            set_list=(', '.join([f"{key} = '{set_list[key]}'" for key in set_list])),
            client_id=str(client_id)
        ))
        params = tuple(set_list.values())

        self.query(sql_update, params)
        self.conn.commit()

    def get_client_id(self, first_name=None, last_name=None, email=None, phone=None):
        client_info = {}

        if first_name:
            client_info['c.first_name'] = first_name
        if last_name:
            client_info['c.last_name'] = last_name
        if email:
            client_info['c.email'] = email
        if phone:
            join = 'JOIN phones p on p.client_id = c.id'
            client_info['p.phone'] = phone
        else:
            join = ''

        find_query = 'SELECT c.id FROM clients c {join} {condition};'.format(
            condition='WHERE ' + ('and '.join([f"{key} = %({key})s" for key in client_info])),
            join=join
        )

        client_id = self.query(find_query, client_info, return_value=True)

        return client_id
