from .sql_decorators.private_decorator import Private
from .sql_decorators.argument_decorator import RequiredV3
from .sql_decorators.timer_decorator import timer, timerV2
from .sql_decorators.file_extension_decorator import extension_decorator
from .sql_exceptions.my_exceptions import ConnectionFieldError, FieldError, \
     NoFileError, WrongFileError
from .data_base_engines import SqliteClient, PostgresClient, MysqlClient
from .sql_decorators.column_decorator import column_decorator
from .file_operators import (csv_operations, json_operations, text_operations)


class FileSystem:
    switch = {'csv': csv_operations.Csv_operations,
              'json': json_operations.Json_operations,
              'txt': text_operations.Text_operations}

    def __get__(self, instance, owner):
        try:
            return self.switch[instance.file_ending]
        except Exception as e:
            print(e)
            raise

    def __set__(*args):
        raise AttributeError('cannot set values for this attribute')


@Private('db_options',)
class DbQueries:
    """ run familiar db queries with ease """
    file_system = FileSystem()
    db_options = {'mysql': [{'db', 'host', 'passwd', 'user'}, MysqlClient],
                  'postgres': [{'database', 'host', 'password', 'port',
                                'user'}, PostgresClient],
                  'sqlite': [{'database'}, SqliteClient]}

    @timer
    def __init__(self, db_choice=list(db_options.keys())[2], **kwargs):
        self.db_choice = db_choice
        if self.db_choice not in self.db_options.keys():
            raise FieldError(self.db_choice, list(self.db_options.keys()))

        self.supplied_connect_fields = kwargs
        self.connection_parameters_test()
        choice = self.db_options[self.db_choice][1]
        self.db_engine = choice(**self.supplied_connect_fields)
        self.cursor = self.db_engine.cursor
        self.tags = self.db_engine.tags
        self.db_conn = self.db_engine.db_connection

    @timer
    def display_query(self, data):
        print(("--" * 5) + ' Query GENERATED ' + ("--" * 5))
        print(data)

    @timer
    def get_file_data(self, data, category='data'):
        if isinstance(data, str):
            self.file_ending = data.split('.')[-1]
            if self.file_ending in ['csv', 'txt', 'json']:
                file_operator = self.file_system
                file_object = file_operator(data)  # singleton instance
                read_file = file_object.read()
                return read_file
            else:
                return data
        else:
            if category == 'data':
                return data
            else:
                raise NoFileError

    @timer
    def connection_parameters_test(self):
        self.default_connect_fields = self.db_options[self.db_choice][0]
        dcf = self.default_connect_fields
        match = len(dcf) == len(dcf.intersection(self.supplied_connect_fields))
        if not match:
            e = set(self.supplied_connect_fields.keys())
            error_fields = e.symmetric_difference(self.default_connect_fields)
            if len(self.default_connect_fields) == 1:
                error_fields.remove(list(self.default_connect_fields)[0])
            raise ConnectionFieldError(self.db_choice, error_fields,
                                       self.default_connect_fields)
        else:
            print(("--" * 5) + ' Connection Fields ' + ("--" * 5))
            for i in self.supplied_connect_fields:
                print(f"{i} : {self.supplied_connect_fields[i]}")

    @RequiredV3(*[('table_name',)])
    @timerV2
    def get_all_columns(self, **kwargs):
        '''w'''
        table_name = kwargs.get('table_name')
        self.select_data(table_name=table_name)
        if self.db_choice == 'sqlite':
            sq_columns = [row[0] for row in self.cursor.description]
            return sq_columns
        self.cursor.execute("desc " + table_name)
        table_columns = [column[0] for column in self.cursor.fetchall()]
        return table_columns

    @RequiredV3(*[('table_name', 'data_file',), ('query_file',),
                ('query_statement',), ('table_name', 'raw_table_data')])
    @timerV2
    def create_table(self, **kwargs):
        ''' this helps to create a table with the specified table name w '''
        table_name = kwargs.get('table_name')
        raw_table_data = kwargs.get('raw_table_data')
        data_file = kwargs.get('data_file')
        query_statement = kwargs.get('query_statement')
        query_file = kwargs.get('query_file')

        def mix(row):
            layer1 = row[0] + ' ' + row[1]
            layer1 += ((' ' + row[2]) if len(row) > 2 else '')
            return layer1

        if not query_statement and not query_file:
            if data_file:
                if data_file.split('.')[-1] != 'csv':
                    raise WrongFileError('.' + data_file.split('.')[-1], '.csv')
                data = self.get_file_data(data_file, 'query')
            else:
                if raw_table_data:
                    data = self.get_file_data(raw_table_data)

            table_parameters = tuple([mix(row) for row in data])
            query_statement = 'CREATE TABLE ' + table_name + ' '
            query_statement += str(table_parameters).replace("'", '')

        try:
            if query_file:
                data = self.get_file_data(query_file)
                query_statement = data
            self.display_query(query_statement)
            self.cursor.execute(query_statement)

        except Exception as e:
            print(e, 'my error')

    @RequiredV3(*[('table_name', 'data_file',), ('query_file',),
                ('query_statement',), ('table_name', 'raw_table_data')])
    @timerV2
    def add_column_to_table(self, **kwargs):
        ''' this helps to add additional columns
            to an already existing database '''
        table_name = kwargs.get('table_name')
        raw_table_data = kwargs.get('raw_table_data')
        data_file = kwargs.get('data_file')
        query_statement = kwargs.get('query_statement')
        query_file = kwargs.get('query_file')

        if not query_statement and not query_file:
            if data_file:
                if data_file.split('.')[-1] != 'csv':
                    raise WrongFileError('.' + data_file.split('.')[-1], '.csv')
                data = self.get_file_data(data_file, 'query')
            else:
                if raw_table_data:
                    data = self.get_file_data(raw_table_data)

            et = 'ALTER TABLE ' + table_name + ' ' + 'ADD COLUMN '
            et += data[0][0] + ' ' + data[0][1] + ' ' + data[0][2] + ', \n'
            query_statement = et

            for i in range(1, len(data)):
                query_statement += 'ADD COLUMN ' + data[i][0] + ' '
                query_statement += data[i][1] + ' ' + data[i][2] + ', \n'
            del data

        try:
            if query_file:
                query_statement = self.get_file_data(query_file)
            self.display_query(query_statement)
            self.cursor.execute(query_statement)

        except Exception as e:
            print(e)

    @timer
    @extension_decorator
    @RequiredV3(*[('table_name', 'values', 'col_names'), ('query_file',),
                ('query_statement',), ('table_name', 'data_file',)])
    def insert_into_table(self, **kwargs):
        ''' this is used to add data to the table,
            either a single row or many rows of data w'''
        table_name = kwargs.get('table_name')
        data_file = kwargs.get('data_file')
        col_names = kwargs.get('col_names')
        values = kwargs.get('values')
        query_statement = kwargs.get('query_statement')
        query_file = kwargs.get('query_file')

        if (values or data_file) and not query_file:
            data = self.get_file_data(data_file, 'data')
            if data:
                values = data.get('values')
                col_names = data.get('column_names')
            query_statement = "INSERT INTO " + table_name + ' '
            query_statement += str(tuple(col_names)).replace("'", '') + ' '
            query_statement += 'VALUES '
            query_statement += str(self.tags[0] * len(col_names)).replace("'", '')
            print(self.tags[0])
            if isinstance(values[0], tuple) or isinstance(values[0], list):
                values = [tuple(v) for v in values]
                self.display_query(query_statement % values[0])
                try:
                    # print(query_statement)
                    self.cursor.executemany(query_statement, values)
                except Exception as e:
                    print(e)

            else:
                values = tuple(values)
                self.display_query(query_statement)
                try:
                    # print(query_statement)
                    self.cursor.execute(query_statement, values)
                except Exception as e:
                    print(e)

        else:
            try:
                if query_file or query_statement:
                    gt = self.get_file_data
                    query_statement = gt(query_file, 'query') if query_file else query_statement
                    self.display_query(query_statement)
                    self.cursor.execute(query_statement)
                else:
                    self.cursor.execute(query_statement, values)
            except Exception as e:
                print(e)

        self.db_conn.commit()
        print(self.cursor.rowcount, " record inserted.")
        print("Last record inserted, ID: ", self.cursor.lastrowid)

    @timerV2
    @extension_decorator
    @RequiredV3(*[('table_name', 'values', 'col_names', 'target'), ('query_file',),
                ('query_statement',)])
    def update_table(self, **kwargs):
        ''' this is used to change data in
            a particular column to a new value '''
        table_name = kwargs.get('table_name')
        query_file = kwargs.get('query_file')
        col_names = kwargs.get('col_names')
        values = kwargs.get('values')
        target = kwargs.get('target')
        query_statement = kwargs.get('query_statement')
        wherex = kwargs.get('where_statement')

        values = tuple(values)
        if not query_statement and not query_file:
            if isinstance(col_names, tuple) or isinstance(col_names, list):
                sql = "UPDATE " + table_name + " SET "
                r_c = str(tuple([i + self.tags[2] for i in col_names]))
                redef_col = r_c.replace("'", '').replace('(', '').replace(')', '')
                sql += redef_col + ' where ' + target + self.tags[1]

            else:
                sql = "UPDATE " + table_name + " SET " + col_names + self.tags[2]
                sql += ("WHERE " + target + self.tags[1]) if not wherex else wherex

            query_statement = sql
        try:
            if query_file or query_statement:
                query_statement = self.get_file_data(query_file) if query_file else query_statement
                self.display_query(query_statement)
                self.cursor.execute(query_statement)
            else:
                self.display_query(query_statement)
                self.cursor.execute(query_statement, values)

            self.db_conn.commit()
            print(self.cursor.rowcount, "record(s) affected")

        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @column_decorator
    @RequiredV3(*[('table_name',), ('query_file',), ('query_statement',)])
    def select_data(self, **kwargs):
        ''' this helps to select all the data or just a single
            row of  data from the database w'''
        table_name = kwargs.get('table_name')
        query_file = kwargs.get('query_file')
        col_names = kwargs.get('col_names')
        query_statement = kwargs.get('query_statement')
        single = kwargs.get('fetch_one')
        if not query_statement and not query_file:
            sql = "SELECT " + col_names + " FROM " + table_name
            query_statement = sql
        try:
            if query_file:
                query_statement = self.get_file_data(query_file)

            self.display_query(query_statement)
            self.cursor.execute(query_statement)
            all_data = self.cursor.fetchall()
            myresult = all_data if not single else self.cursor.fetchone()
            return myresult

        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @column_decorator
    @RequiredV3(*[('table_name', 'col_names', 'column', 'search_value'), ('query_file',),
                ('query_statement',)])
    def select_where(self, **kwargs):
        ''' this gets all the data  that matches
            a value from a particular column w'''
        table_name = kwargs.get('table_name')
        column = kwargs.get('column')
        search_value = kwargs.get('search_value')
        query_file = kwargs.get('query_file')
        col_names = kwargs.get('col_names')
        query_statement = kwargs.get('query_statement')
        single = kwargs.get('fetch_one')
        wherestatement = kwargs.get('wherestatement')

        if not query_statement and not query_file:
            var = []
            var.append(search_value)
            search_value = tuple(var)
            ws = wherestatement
            print(search_value)
            sql = "SELECT " + col_names + " FROM " + table_name + ' '
            sql += ('WHERE ' + column + ' = ' + '?') if not ws else ws
            query_statement = sql

        try:
            if query_file or query_statement:
                query_statement = self.get_file_data(query_file) if query_file else query_statement
                self.display_query(query_statement)
                self.cursor.execute(query_statement)
            else:
                self.display_query(query_statement%search_value)
                self.cursor.execute(query_statement, search_value)
            all_data = self.cursor.fetchall()
            myresult = all_data if not single else self.cursor.fetchone()

            return myresult

        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @column_decorator
    @RequiredV3(*[('table_name', 'col_names', 'column', 'search_value'), ('query_file',),
                ('query_statement',)])
    def select_where_like(self, **kwargs):
        ''' this gets all the data  that match a similar
            value from a particular column w'''
        table_name = kwargs.get('table_name')
        column = kwargs.get('column')
        search_value = kwargs.get('search_value')
        query_file = kwargs.get('query_file')
        col_names = kwargs.get('col_names')
        query_statement = kwargs.get('query_statement')
        single = kwargs.get('fetch_one')

        if not query_statement and not query_file:
            sql = "SELECT " + col_names + " FROM " + table_name + ' WHERE '
            sql += column + ' LIKE ' + '\"%' + str(search_value) + '%\"'
            query_statement = sql

        try:
            if query_file:
                query_statement = self.get_file_data(query_file)
            self.display_query(query_statement)
            self.cursor.execute(query_statement)
            all_data = self.cursor.fetchall()
            myresult = all_data if not single else self.cursor.fetchone()

            return myresult

        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @column_decorator
    @RequiredV3(*[('table_name', 'col_names', 'column'), ('query_file',), ('query_statement',)])
    def select_and_orderby(self, **kwargs):
        ''' this helps you to oder your selection
            based on the required column w'''
        table_name = kwargs.get('table_name')
        query_file = kwargs.get('query_file')
        query_statement = kwargs.get('query_statement')
        column = kwargs.get('column')
        col_names = kwargs.get('col_names')
        single = kwargs.get('fetch_one')

        if not query_statement and not query_file:
            sql = "SELECT " + col_names + " FROM " + table_name
            sql += ' ORDER BY ' + column
            query_statement = sql

        try:
            if query_file:
                query_statement = self.get_file_data(query_file)

            self.display_query(query_statement)
            self.cursor.execute(query_statement)
            all_data = self.cursor.fetchall()
            myresult = all_data if not single else self.cursor.fetchone()
            return myresult

        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @column_decorator
    @RequiredV3(*[('table_name', 'col_names', 'limit_value'), ('query_file',),
                ('query_statement',)])
    def select_and_limitby(self, **kwargs):
        ''' this helps you to get a specific amount of
            rows from the table w'''
        table_name = kwargs.get('table_name')
        query_file = kwargs.get('query_file')
        query_statement = kwargs.get('query_statement')
        limit_value = kwargs.get('limit_value')
        col_names = kwargs.get('col_names')
        single = kwargs.get('fetch_one')

        if not query_statement and not query_file:
            sql = "SELECT " + col_names + " FROM " + table_name
            sql += ' LIMIT ' + str(limit_value)
            query_statement = sql

        try:
            if query_file:
                query_statement = self.get_file_data(query_file)

            self.display_query(query_statement)
            self.cursor.execute(query_statement)
            all_data = self.cursor.fetchall()
            myresult = all_data if not single else self.cursor.fetchone()
            return myresult

        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @column_decorator
    @RequiredV3(*[('table_name', 'col_names', 'limit_start', 'limit_value'), ('query_file',),
                ('query_statement',)])
    def select_and_limitbyn(self, **kwargs):
        ''' this helps you to get a specific amount of rows from
            the table starting from a specific row  w'''
        table_name = kwargs.get('table_name')
        query_file = kwargs.get('query_file')
        query_statement = kwargs.get('query_statement')
        limit_start = kwargs.get('limit_start')
        limit_value = kwargs.get('limit_value')
        col_names = kwargs.get('col_names')
        single = kwargs.get('fetch_one')

        if not query_statement and not query_file:
            sql = "SELECT " + col_names + " FROM " + table_name + ' LIMIT '
            sql += str(limit_value) + ' OFFSET ' + str(limit_start)
            query_statement = sql

        try:
            if query_file:
                query_statement = self.get_file_data(query_file)

            self.display_query(query_statement)
            self.cursor.execute(query_statement)
            all_data = self.cursor.fetchall()
            myresult = all_data if not single else self.cursor.fetchone()
            return myresult

        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @RequiredV3(*[('table_name', 'column'), ('query_file',), ('query_statement',)])
    def select_and_orderby_column(self, **kwargs):
        ''' this helps you to oder your selection
            based on the required column w'''
        table_name = kwargs.get('table_name')
        column = kwargs.get('column')
        single = kwargs.get('fetch_one')
        query_file = kwargs.get('query_file')
        query_statement = kwargs.get('query_statement')

        if not query_statement and not query_file:
            sql = "SELECT * FROM " + table_name + ' ORDER BY ' + column + ' DESC'
            query_statement = sql
        try:
            if query_file:
                query_statement = self.get_file_data(query_file)

            self.display_query(query_statement)
            self.cursor.execute(query_statement)
            all_data = self.cursor.fetchall()
            myresult = all_data if not single else self.cursor.fetchone()
            return myresult
        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @RequiredV3(*[('table_name', 'search_value', 'column'), ('query_file',), ('query_statement',)])
    def delete_data(self, **kwargs):
        ''' this is used to delete a row  data
            from a table based on the column w '''
        table_name = kwargs.get('table_name')
        column = kwargs.get('column')
        search_value = kwargs.get('search_value')
        query_file = kwargs.get('query_file')
        query_statement = kwargs.get('query_statement')
        where_statement = kwargs.get('where_statement')

        if not query_statement and not query_file:
            var = []
            var.append(search_value)
            search_value = tuple(var)
            sql = "DELETE * FROM " + table_name + ' '
            sql += ('WHERE ' + column + self.tags[1]) if not where_statement else where_statement
            query_statement = sql

        try:
            if query_file or query_statement:
                query_statement = self.get_file_data(query_file) if query_file else query_statement
                self.display_query(query_statement)
                self.cursor.execute(query_statement)
            else:
                self.display_query(query_statement)
                self.cursor.execute(query_statement, search_value)
            self.db_conn.commit()
            print(self.cursor.rowcount, "record(s) deleted")
        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @RequiredV3(*[('table_name',), ('query_file',), ('query_statement',)])
    def delete_table(self, **kwargs):
        ''' this is used to delete a table from the database '''
        table_name = kwargs.get('table_name')
        query_file = kwargs.get('query_file')
        query_statement = kwargs.get('query_statement')
        if not query_statement and not query_file:
            query_statement = "DROP TABLE IF EXISTS " + table_name

        try:
            if query_file:
                query_statement = self.get_file_data(query_file)
            self.display_query(query_statement)
            self.cursor.execute(query_statement)
            self.cursor.execute("SHOW TABLES")
            for x in self.cursor:
                print(x)

        except Exception as e:
            print(e)

    @timerV2
    @extension_decorator
    @RequiredV3(*[('query_file',), ('query_statement',)])
    def plain_queries(self, **kwargs):
        ''' this is used to execute other types of database queries not covered '''
        query_file = kwargs.get('query_file')
        query_statement = kwargs.get('query_statement')

        try:
            if query_file:
                query_statement = self.get_file_data(query_file)
            self.display_query(query_statement)
            self.cursor.execute(query_statement)

        except Exception as e:
            print(e)

        else:
            self.db_conn.commit()

    def close_connection(self):
        self.cursor.close()
        self.db_conn.close()
        print(' All database connections closed !!!')
