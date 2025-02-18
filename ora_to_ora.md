### cx_Oracle, sqlAlchemy
- cx_Oracle 단독으로는 read_sql은 되지만 to_sql은 안된다.
- sqlalchemy engine을 사용해야한다.  
```python
import cx_Oracle
import datetime
import pandas as pd
from sqlalchemy import create_engine

source_host = "localhost"
port = 1521
source_user = "TESTSCHEMA"
source_pwd = "TESTSCHEMA"
source_svc_name = 'YONG'

conn_string = f"oracle+cx_oracle://{source_user}:{source_pwd}@{source_host}:{port}/?service_name={source_svc_name}"
engine =  create_engine(conn_string)
with engine.connect() as conn:
    data = pd.read_sql('select * from TESTSCHEMA.PY_INSERT_TEST', conn)
    
target_host = "localhost"
port = 1521
target_svc_name = 'YONG'
target_user ='IF_LAKE_ETL'
target_pwd = 'IF_LAKE_ETL'

conn_string = f"oracle+cx_oracle://{target_user}:{target_pwd}@{target_host}:{port}/?service_name={target_svc_name}"
engine =  create_engine(conn_string)
with engine.connect() as conn:
    inserted_rows = data.to_sql(name ='py_insert_test',schema='TESTSCHEMA',con = conn, if_exists = 'append', index = False)
```
### auto commit 방지
- transaction이 시작되지 않았으면 to_sql은 자동으로 commit 을 실행한다.
- 이를 방지하려면 명시적으로 트랜잭션을 시작해야한다.
```python
with engine.connect() as conn:
    trans = conn.begin()  # 트랜잭션 시작
    try:
        inserted_rows = data.to_sql(name='py_insert_test', schema='TESTSCHEMA', con=conn, if_exists='append', index=False)
        trans.commit()  # 명시적 COMMIT
    except Exception as e:
        trans.rollback()  # 예외 발생 시 ROLLBACK
```
- begin()을 사용하면 위를 간단하게 할 수 있다.
```python
with engine.begin() as conn:  # 트랜잭션 자동 시작
    data.to_sql(name='py_insert_test', schema='TESTSCHEMA', con=conn, if_exists='append', index=False)
# 블록이 끝나면 자동으로 COMMIT (예외가 없으면)
```





```python
"""
old
"""
import cx_Oracle

# Oracle 소스 및 타겟 데이터베이스 연결 설정
source_conn = cx_Oracle.connect('source_user', 'source_password', 'source_host:source_port/source_service')
target_conn = cx_Oracle.connect('target_user', 'target_password', 'target_host:target_port/target_service')

# 소스 커서와 타겟 커서 생성
source_cursor = source_conn.cursor()
target_cursor = target_conn.cursor()

try:
    # 1. 소스 DB에서 데이터 선택
    source_cursor.execute("SELECT column1, column2, column3 FROM source_table WHERE condition = 'value'")
    rows = source_cursor.fetchall()

    # 2. 타겟 DB에 데이터 삽입
    insert_query = "INSERT INTO target_table (column1, column2, column3) VALUES (:1, :2, :3)"

    # 여러 데이터를 한번에 삽입
    target_cursor.executemany(insert_query, rows)

    # 트랜잭션 커밋
    target_conn.commit()
    print(f"{len(rows)} rows have been inserted successfully.")

except cx_Oracle.DatabaseError as e:
    print(f"An error occurred: {e}")
    target_conn.rollback()

finally:
    # 커서와 연결 닫기
    source_cursor.close()
    target_cursor.close()
    source_conn.close()
    target_conn.close()
```
