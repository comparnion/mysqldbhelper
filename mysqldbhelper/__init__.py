import MySQLdb
import json

def json_output(obj):
    return json.dumps(obj, default=date_handler, sort_keys=True,
            indent=4)

def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj

class DatabaseConnection:
    def __init__(self, host, user, passwd, db):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db

    def connect(self):
        try:
            self.connection = MySQLdb.connect(host=self.host,
                    user=self.user,
                    passwd=self.passwd,
                    db=self.db)

            self.cur = self.connection.cursor()
            return self.cur
        except Exception, e:
            raise

    def disconnect(self):
        self.connection.close()
        self.cur.close()

    def get_one(self, qry, tpl):
        try:
            cur = self.connect()
            cur.execute(qry + ' LIMIT 1', tpl)
            result = cur.fetchone()
            self.disconnect()
            # unpack tuple if it has only
            # one element
            if type(result) is tuple and len(result) == 1:
                result = result[0]
            return result
        except Exception, e:
            raise

    def get_all(self, qry, tpl):
        try:
            cur = self.connect()
            cur.execute(qry, tpl)
            result = cur.fetchall()
            self.disconnect()
            return result
        except Exception, e:
            raise

    def put(self, qry, tpl):
        try:
            cur = self.connect()
            cur.execute(qry, tpl)
            self.connection.commit()
            self.disconnect()
        except Exception, e:
            raise
