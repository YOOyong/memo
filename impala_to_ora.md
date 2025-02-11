# impala to oracle
- jaydebeapi 를 쓰니 쿼리 결과 타입 확인 필요.
- cx_Oracle 에서 insert 할 수 있도록 형변환 필요. (datetime 등)


```python
import jaydebeapi
import cx_Oracle
import datetime

# Impala 연결 정보 (jaydebeapi 사용)
impala_driver_name = "com.cloudera.impala.jdbc41.Driver"
impala_driver_loc = "/path/to/impala-jdbc-driver.jar"
impala_connection_string = "jdbc:impala://impala_host:21050"
impala_user = "your_impala_user"
impala_password = "your_impala_password"

# Impala 연결
impala_conn = jaydebeapi.connect(
    impala_driver_name, impala_connection_string, [impala_user, impala_password], impala_driver_loc
)
impala_cursor = impala_conn.cursor()

# Oracle 연결 정보 (cx_Oracle 사용)
oracle_conn = cx_Oracle.connect("oracle_user", "oracle_password", "oracle_host:oracle_port/oracle_service")
oracle_cursor = oracle_conn.cursor()

def convert_java_to_python(value, java_type):
    """
    Java 객체 타입을 Python 기본 타입으로 변환하는 함수
    """
    if isinstance(value, jaydebeapi._java._jint):  # Java Integer -> Python int
        return int(value)
    elif isinstance(value, jaydebeapi._java._jdouble):  # Java Double -> Python float
        return float(value)
    elif isinstance(value, jaydebeapi._java._jlong):  # Java Long -> Python int
        return int(value)
    elif isinstance(value, jaydebeapi._java._jstring):  # Java String -> Python str
        return str(value)
    elif isinstance(value, jaydebeapi._java._jdate):  # Java Date -> Python datetime.date
        return value
    elif isinstance(value, jaydebeapi._java._jtimestamp):  # Java Timestamp -> Python datetime.datetime
        return value
    else:
        # 기타 타입은 변환하지 않고 그대로 반환
        return value


try:
    # Impala에서 데이터 가져오기
    impala_cursor.execute("SELECT column1, column2, column3, date_column, number_column FROM impala_table WHERE condition = 'value'")

    # 컬럼 메타데이터 확인 (데이터 타입 확인)
    columns_info = impala_cursor.description
    for column in columns_info:
        column_name = column[0]
        column_type = column[1]
        print(f"Column name: {column_name}, Column type: {column_type}")

    # 데이터 가져오기
    rows = impala_cursor.fetchall()

    # 데이터를 변환하여 처리
    transformed_rows = []
    for row in rows:
        transformed_row = []
        for i, value in enumerate(row):
            column_name = columns_info[i][0]
            column_type = columns_info[i][1]

            # Java 객체를 Python 기본 타입으로 변환
            value = convert_java_to_python(value, column_type)

            # 날짜 형식 변환 (string -> datetime)
            if isinstance(value, str):
                try:
                    # 문자열이 날짜 형식일 경우 datetime으로 변환
                    value = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass  # 날짜 형식이 아니면 그대로 둔다

            transformed_row.append(value)

        transformed_rows.append(tuple(transformed_row))

    # Oracle 테이블에 데이터 삽입
    insert_query = "INSERT INTO oracle_table (column1, column2, column3, date_column, number_column) VALUES (:1, :2, :3, :4, :5)"
    oracle_cursor.executemany(insert_query, transformed_rows)

    # 커밋하여 변경 사항 저장
    oracle_conn.commit()

    print(f"{len(transformed_rows)} rows successfully inserted into Oracle.")

except Exception as e:
    print(f"An error occurred: {e}")
    oracle_conn.rollback()

finally:
    # 커서 및 연결 닫기
    impala_cursor.close()
    oracle_cursor.close()
    impala_conn.close()
    oracle_conn.close()

```



```python
import jaydebeapi
import cx_Oracle

# Impala 연결 정보 (jaydebeapi 사용)
impala_driver_name = "com.cloudera.impala.jdbc41.Driver"
impala_driver_loc = "/path/to/impala-jdbc-driver.jar"
impala_connection_string = "jdbc:impala://impala_host:21050"
impala_user = "your_impala_user"
impala_password = "your_impala_password"

# Impala 연결
impala_conn = jaydebeapi.connect(
    impala_driver_name, impala_connection_string, [impala_user, impala_password], impala_driver_loc
)
impala_cursor = impala_conn.cursor()

# Oracle 연결 정보 (cx_Oracle 사용)
oracle_conn = cx_Oracle.connect("oracle_user", "oracle_password", "oracle_host:oracle_port/oracle_service")
oracle_cursor = oracle_conn.cursor()

try:
    # Impala에서 데이터 가져오기
    impala_cursor.execute("SELECT column1, column2, column3, date_column, number_column FROM impala_table WHERE condition = 'value'")
    rows = impala_cursor.fetchall()

    # 데이터를 변환하여 처리
    transformed_rows = []
    for row in rows:
        column1 = row[0]  # 예: int (Java Integer -> Python int로 변환)
        column2 = row[1]  # 예: string
        column3 = row[2]  # 예: string
        date_column = row[3]  # 예: 날짜
        number_column = row[4]  # 예: 숫자 (Java Integer/Double -> Python float 또는 int로 변환)

        # Java 숫자 타입 (Integer, Double)을 Python 숫자 타입 (int, float)으로 변환
        if isinstance(number_column, (int, float)):
            # 이미 int 또는 float이면 그대로 사용
            number_column = number_column
        elif isinstance(number_column, jaydebeapi._java._jlong):  # Impala에서 long 형식일 경우
            number_column = int(number_column)  # long은 Python의 int로 변환
        elif isinstance(number_column, jaydebeapi._java._jdouble):  # Impala에서 double 형식일 경우
            number_column = float(number_column)  # double은 Python의 float으로 변환
        elif isinstance(number_column, jaydebeapi._java._jint):  # Impala에서 integer 형식일 경우
            number_column = int(number_column)  # Java Integer를 Python int로 변환

        transformed_rows.append((column1, column2, column3, date_column, number_column))

    # Oracle 테이블에 데이터 삽입
    insert_query = "INSERT INTO oracle_table (column1, column2, column3, date_column, number_column) VALUES (:1, :2, :3, :4, :5)"
    oracle_cursor.executemany(insert_query, transformed_rows)

    # 커밋하여 변경 사항 저장
    oracle_conn.commit()

    print(f"{len(transformed_rows)} rows successfully inserted into Oracle.")

except Exception as e:
    print(f"An error occurred: {e}")
    oracle_conn.rollback()

finally:
    # 커서 및 연결 닫기
    impala_cursor.close()
    oracle_cursor.close()
    impala_conn.close()
    oracle_conn.close()
```
