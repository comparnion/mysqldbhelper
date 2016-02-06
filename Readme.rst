MySqlDbHelper
=============

For use with the mysqldb module for python

Usage

::

    import mysqldbhelper
    db = mysqldbhelper.DatabaseConnection('hostname',
                        user='username',
                        passwd='password',
                        db='databasename')

For single transactions
-----------------------

::

    db.get_one('select book_name from book where isbn = %s', ('SOMEISBN',))

‘limit 1’ is automatically added

::

    db.get_all('select book_name from book where author = %s', ('Richard Dawkins',))

::

    db.put('''
    insert into book
    (book_name, book_author)
    values
    (%s, %s)''', ('Phantoms in the brain', 'V.S. Ramachandran')

put can be used for insert, update and delete queries

::

    db.put('''delete from book
    where
    book_author = %s''' ('Deepak Chopra',))

For multiple transcations that need to be run atomically
--------------------------------------------------------

::

    try:
        db.start()
        db.get(...)
        db.put(...)
        ...
        db.save()
    except Exception, e:
        db.rollback()
        raise

Used at `Comparnion.com`_

.. _Comparnion.com: https://comparnion.com
