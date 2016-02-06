#!/usr/local/bin/python
import MySQLdb
import json

def json_output(obj):
    return json.dumps(obj, default=date_handler, sort_keys=True,
            indent=4)

def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

class DatabaseConnection:
    def __init__(self, host, user, passwd, db, charset='utf8'):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset = charset
        # runs every query atomically if true
        # start sets atomic to false so that multiple
        # queries can be executed
        # if start is called save needs to be called to commit
        # transactions
        self.atomic = True

    def connect(self):
        try:
            self.connection = MySQLdb.connect(host=self.host,
                    user=self.user,
                    passwd=self.passwd,
                    db=self.db,
                    charset=self.charset)

            self.cur = self.connection.cursor()
        except Exception, e:
            raise

    def disconnect(self):
        self.connection.close()
        self.cur.close()

    def atomize(f):
        """ @ atomize decorator
        creates two versions of a function
        for atomic and not atomic queries"""

        def wrapper(*args, **kwargs):
            db = args[0]
            def if_atomic(*args, **kwargs):
                try:
                    db.connect()
                    result = f(*args, **kwargs)
                    db.connection.commit()
                    db.disconnect()
                    return result
                except Exception, e:
                    db.rollback()
                    raise

            if db.atomic:
                return if_atomic(*args, **kwargs)
            else:
                return f(*args, **kwargs)
        return wrapper

    @atomize
    #TODO tpl should be optional ()
    def get_one(self, qry, tpl):
        ''' get a single from from a query
        limit 1 is automatically added '''
        self.cur.execute(qry + ' LIMIT 1', tpl)
        result = self.cur.fetchone()
        # unpack tuple if it has only
        # one element
        # TODO unpack results
        if type(result) is tuple and len(result) == 1:
            result = result[0]
        return result

    @atomize
    def get_all(self, qry, tpl):
        ''' get all rows for a query '''
        self.cur.execute(qry, tpl)
        result = self.cur.fetchall()
        return result

    @atomize
    def put(self, qry, tpl):
        ''' insert, update or delete query '''
        self.cur.execute(qry, tpl)

    def start(self):
        self.atomic = False
        self.connect()

    def rollback(self):
        self.connection.rollback()
        self.disconnect()
        self.atomic = True

    def save(self):
        self.connection.commit()
        self.disconnect()
        self.atomic = True
