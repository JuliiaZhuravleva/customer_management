import psycopg2


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
        new_attributes = {'client_id': client_id}
        set_list = 'SET '

        if first_name:
            new_attributes['first_name'] = first_name
            set_list += 'first_name = %(first_name)s'
        if last_name:
            new_attributes['last_name'] = last_name
            if set_list[-1] == 's':
                set_list += ', '
            set_list += 'last_name = %(last_name)s'
        if email:
            new_attributes['email'] = email
            if set_list[-1] == 's':
                set_list += ', '
            set_list += 'email = %(email)s'
        sql_update = 'UPDATE clients ' + set_list + ' WHERE id = %(client_id)s;'

        self.query(sql_update, new_attributes)
        self.conn.commit()

    def get_client_id(self, first_name=None, last_name=None, email=None, phone=None):
        client_info = {}
        where_condition = ' WHERE 1=1'
        if first_name:
            client_info['first_name'] = first_name
            where_condition += ' AND first_name = %(first_name)s'
        if last_name:
            client_info['last_name'] = last_name
            where_condition += ' AND last_name = %(last_name)s'
        if email:
            client_info['email'] = email
            where_condition += ' AND email = %(email)s'

        client_ids_from_clients = self.query('SELECT id FROM clients' + where_condition, client_info, return_values=True)

        if phone:
            client_ids_from_phones = {self.query(
                'SELECT client_id FROM phones WHERE phone = %(phone)s',
                {'phone': phone},
                return_value=True), }
            client_ids_from_clients = set(client_ids_from_clients) & client_ids_from_phones

        client_ids = []
            
        for client_id in client_ids_from_clients:
            client_ids.append(int(client_id[0]))

        return client_ids
