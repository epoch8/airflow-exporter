'''functions for communicating with the database'''
from contextlib import contextmanager

from airflow.settings import Session


@contextmanager
def session_scope():
    """
    Provide a transactional scope around a series of operations.
    """
    session = Session
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

def get_context():
    """
    get functor for connect to db
    :param db_name: name db for connect
    :return:
    """
    def attach_conn_context(function):
        def wrapper(*args, **kwargs):
            # noinspection PyUnusedLocal
            with session_scope() as session:
                return function(*args, **kwargs, connection=session)

        return wrapper

    return attach_conn_context
