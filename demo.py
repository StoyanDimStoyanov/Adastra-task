import json
import os.path
import random
import sys
import psycopg2
from datetime import datetime, timezone


class ETL:

    def __init__(self):
        self.data = None
        self.where_to_sink = ''
        self.source_name = ''
        self.file_path = ''

        """"Enter your data base credentials """

        self.host = 'localhost'
        self.data_base = 'Adastra_test'
        self.user = ''
        self.password = ''

    def source(self, *args):
        if (args[0] != 'Simulation' or args[0] != 'simulation') and os.path.isfile(args[0]):
            self.file_path = args[0]
            self.source_name = 'file'
        return self

    def sink(self, *args):
        self.where_to_sink = args[0]
        return self

    def run(self):
        if self.source_name == 'file':
            with open(self.file_path, 'r') as file:
                self.data = json.load(file)
            if self.where_to_sink.lower() == 'console':
                for obj in self.data:
                    sys.stdout.write(f'{obj}\n')
            else:
                [self.postgres_conn(x) for x in self.data]
        else:
            if self.where_to_sink.lower() == 'console':
                sys.stdout.write(str(self.simulation()))
            else:

                self.postgres_conn((self.simulation()))

    def postgres_conn(self, data):

        con = psycopg2.connect(
            host=self.host,
            database=self.data_base,
            user=self.user,
            password=self.password
        )
        cur = con.cursor()
        cur.execute("select * from information_schema.tables where table_name=%s", ('table3',))
        if bool(cur.rowcount):
            cur.execute("""
                            INSERT into table3 (key, value, ts) values (%s, %s, %s)
                        """, (data['key'], data['value'], data['ts']))
        else:
            cur.execute("""CREATE TABLE table3 (
                    key  VARCHAR(20),
                    value DOUBLE  PRECISION,
                    ts TIMESTAMP WITH TIME ZONE);
            """)
            cur.execute("""
                INSERT into table3 (key, value, ts) values (%s, %s, %s)
            """, (data['key'], data['value'],  data['ts']))

        con.commit()
        cur.close()
        con.close()
        return

    """Simulation return json"""
    @staticmethod
    def simulation():
        time = datetime.now(timezone.utc)
        key = f"{chr(random.randint(65, 90))}{random.randint(100, 999)}"
        data = {"key": key,
                "value": f"{random.uniform(15, 99):.2f}",
                "ts": f'{time}'}
        return json.loads(json.dumps(data))


if __name__ == '__main__':
    ETL().source("simulation").sink('database').run()
