import psycopg2
from psycopg2 import sql


class MyDatabase:
    def __init__(self, db, user, password):
        self.conn = psycopg2.connect(database=db, user=user, password=password)
        self.cur = self.conn.cursor()
        self.user_attr = {'required': ['first_name', 'last_name', 'email'], 'optional': ['phone']}

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

    def add_client(self, attrs):
        sql_insert_client = '''
        INSERT INTO clients (first_name, last_name, email) 
        values (%(first_name)s, %(last_name)s, %(email)s)
        RETURNING id;
        '''
        client_info = {}
        for attr in self.user_attr['required']:
            if attr not in attrs:
                return f'Не хватает необходимого атрибута {attr}'
            client_info[attr] = attrs[attr]

        client_id = self.query(sql_insert_client, client_info, return_value=True)
        self.conn.commit()

        for attr in self.user_attr['optional']:
            if attr == 'phone' and attr in attrs:
                self.add_phone(client_id, attrs[attr])
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

    def change_client_info(self, client_id, attrs):
        set_list = {}

        for attr in self.user_attr['required']:
            if attr in attrs:
                set_list[attr] = attrs[attr]

        sql_update = sql.SQL("""
                UPDATE clients SET {set_list} WHERE id = {client_id};
                """.format(
            set_list=(', '.join([f"{key} = '{set_list[key]}'" for key in set_list])),
            client_id=str(client_id)
        ))
        params = tuple(set_list.values())

        self.query(sql_update, params)
        self.conn.commit()

    def get_client_id(self, attrs):
        client_info = {}
        join = ''

        for attr in self.user_attr['required']:
            if attr in attrs:
                client_info[f'c.{attr}'] = attrs[attr]

        for attr in self.user_attr['optional']:
            if attr == 'phone' and attr in attrs:
                join = 'JOIN phones p on p.client_id = c.id'
                client_info['p.phone'] = attrs[attr]

        find_query = 'SELECT c.id FROM clients c {join} {condition};'.format(
            condition='WHERE ' + ('and '.join([f"{key} = %({key})s" for key in client_info])),
            join=join
        )

        client_id = self.query(find_query, client_info, return_value=True)

        return client_id
