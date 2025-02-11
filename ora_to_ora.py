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
