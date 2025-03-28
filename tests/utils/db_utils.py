from cnosdb_connector import connect


class Database:

    def __init__(self):
        self.tenant = None
        self.username = None
        self.password = None
        self.db_name = None
        self.url = None
        self.connection = None

    def connect(self):
        conn = connect(url="http://127.0.0.1:8902/", user="root", password="")
        resp = conn.list_database()
        if resp.status_code == 200:
            self.tenant = resp.json()['tenant']





