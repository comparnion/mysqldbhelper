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
            def if_atomic(*args, **kwargs):
                print 'atomic version'
                db = args[0]
                try:
                    db.connect()
                    result = f(*args, **kwargs)
                    db.save()
                    return result
                except Exception, e:
                    db.rollback()
                    raise

            if kwargs and kwargs['atomic'] == False:
                print 'non atomic version'
                return f(*args, **kwargs)
            else:
                return if_atomic(*args, **kwargs)
        return wrapper

    @atomize
    def get_one(self, qry, tpl, atomic=True):
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
    def get_all(self, qry, tpl, atomic=True):
        ''' get all rows for a query '''
        self.cur.execute(qry, tpl)
        result = self.cur.fetchall()
        return result

    @atomize
    ''' insert, update or delete query '''
    def put(self, qry, tpl, atomic=True):
        self.cur.execute(qry, tpl)

    def start(self):
        self.connect()

    def rollback(self):
        #TODO
        pass

    def save(self):
        self.connection.commit()
        self.disconnect()

