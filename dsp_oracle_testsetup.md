```sql
--test db setup

CREATE USER PYINSTEST IDENTIFIED BY PYINSTEST;
CREATE USER IF_LAKE_ETL IDENTIFIED BY IF_LAKE_ETL;

GRANT CONNECT,RESOURCE TO PYINSTEST;
GRANT CONNECT,RESOURCE TO IF_LAKE_ETL;


CREATE TABLE PYINSTEST.INSERT_TEST (
    C1 VARCHAR2(100),
    C2 VARCHAR2(100),
    C3 VARCHAR2(100),
    C4 NUMBER,
    C5 NUMBER,
    C6 NUMBER,
    C7 NUMBER,
    C8 NUMBER,
    C9 DATE,
    C10 TIMESTAMP
);

-- 다른 유저에서 확인해본다.
GRANT SELECT, INSERT, UPDATE, DELETE ON TESTSCHEMA.INSERT_TEST TO IF_LAKE_ETL;

```

### insert test example
```python
import cx_Oracle
import datetime
lib_dir = r"C:\Users\yong\Downloads\instantclient-basic-windows.x64-19.26.0.0.0dbru\instantclient_19_26"
# cx_Oracle.init_oracle_client(lib_dir=lib_dir)


host = "localhost"
port = 1521
user = "TESTSCHEMA"
pwd = "TESTSCHEMA"

"""
 CREATE TABLE TESTSCHEMA.PY_INSERT_TEST (
     C1 VARCHAR2(100),
     C2 VARCHAR2(100),
     C3 VARCHAR2(100),
     C4 NUMBER,
     C5 NUMBER,
     C6 NUMBER,
     C7 NUMBER,
     C8 NUMBER,
     C9 DATE,
     C10 TIMESTAMP
 );
"""
test_data =  [
    ('Text672', 'Text129', 'Text803', 8367, 3941.23, 2568, 7012.89, 3124, datetime.date(2024, 5, 20), datetime.datetime(2024, 7, 15, 14, 32)),
    ('Text421', 'Text594', 'Text678', 473.55, 9420, 6213.67, 8742, 3159.12, datetime.date(2024, 9, 8), datetime.datetime(2024, 10, 10, 3, 15)),
    ('Text985', 'Text732', 'Text256', 1023, 8472.45, 3145, 6578.78, 9823, datetime.date(2024, 3, 14), datetime.datetime(2024, 5, 28, 9, 47)),
    ('Text512', 'Text899', 'Text350', 7514.11, 2367, 9240.99, 8156, 1209.34, datetime.date(2023, 12, 25), datetime.datetime(2024, 1, 30, 22, 5)),
    ('Text237', 'Text674', 'Text489', 3298.66, 1056, 7892, 5610.22, 4732, datetime.date(2024, 6, 3), datetime.datetime(2024, 7, 21, 17, 12))
]

test_data2 = [
    ('Text672', 'Text129', 'Text803', 8367, 3941.23, 2568, 7012.89, 3124, '2024-05-20', '2024-07-15 14:32:00'),
    ('Text672', 'Text129', 'Text803', 8367, 3941.23, 2568, 7012.89, 3124, '2024-05-20', '2024-07-15 14:32:00'),
    ('Text672', 'Text129', 'Text803', 8367, 3941.23, 2568, 7012.89, 3124, '2024-05-20', '2024-07-15 14:32:00'),
    ('Text672', 'Text129', 'Text803', 8367, 3941.23, 2568, 7012.89, 3124, '2024-05-20', '2024-07-15 14:32:00'),
    ('Text672', 'Text129', 'Text803', 8367, 3941.23, 2568, 7012.89, 3124, '2024-05-20', '2024-07-15 14:32:00'),
]


# datetime 객체가 아닌 string으로 왔을 때
# datetime 객체가 들어가면 날짜 잘못들어감.
insert_q  = """
insert into TESTSCHEMA.PY_INSERT_TEST
(C1, C2, C3, C4, C5, C6, C7, C8, C9, C10) 
VALUES (:1, :2, :3, :4, :5, :6, :7, :8, to_date(:9,'YYYY-MM-DD'), TO_TIMESTAMP(:10,'YYYY-MM-DD HH24:MI:SS'))
"""


insert_q_no_col  = """
insert into TESTSCHEMA.PY_INSERT_TEST
VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9,:10)
"""


dsn = cx_Oracle.makedsn(host, port, 'YONG')
with cx_Oracle.connect(user, pwd, dsn) as conn:
    cursor = conn.cursor()
    

    cursor.executemany(insert_q_no_col,test_data)
        
    conn.commit()
```
