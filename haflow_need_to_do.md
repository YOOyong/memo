### 코드 수정
- 하드코딩 되어있는 연결 정보들 airflow variable 을 사용하는 방향으로 수정 -> 협의 필요
 
- 모든 db connection에 명시적 close 추가. 현재 없는 부분 많음. finally로 보장되도록 수정

- 템플릿 유지/보수 쉽도록 공통부분 추출

- 구조 개선 발란???


- \0001 ??? 확인해보기.




# oracle to oracle 대비 리서치

## 방식
- 임시 csv 파일 저장
- csv 파일 read 후, executemany 를 통한 insert


## task 정의

### querying data
- python db api, pandas 활용으로 csv 저장
- read_sql, chunksize 적용
```python
#query 예제코드

def querying_source_data():
    import pymysql
    import pandas as pd
    import os

    temp_csv_file_path = 'test_csv.csv'
    os.remove(temp_csv_file_path, ignore_errors=True)


    conn = pymysql.connect(host='localhost', port='', dbname = 'test', username='test', password='test')
    cursor  = conn.cursor()


    for i, chunk in enumerate(pd.read_sql("select * from test", conn ,chunksize=100000)):
        mode = 'w' if i == 0 else 'a'
        chunk.to_csv('temp_csv.csv', mode = mode, header = None, encoding = 'utf-8', separator = u'\001')

    cursor.close()
    conn.close()

    return 
```




### insert data
- task 중간 실패 고려
- 커밋 필요

```python
# upsert 예제코드
def upsert_data():
    import pymysql
    import pandas as pd
    import os
    import csv

    temp_csv_file_path = 'test_csv.csv'

    # csv 모듈로 column 수 구하기
    with open(temp_csv_file_path, 'r', newline='') as csvfile:
        csvreader = csv.reader(csvfile, delimiter = u'\001', quotechar='"')
        num_columns = len(next(csvreader))

    # 쿼리 생성 컬럼 개수에 맞게
    columns = ['?'] * num_columns
    query  = f"insert into test_table values ({','.join(columns)})"

    config = {
        host : 'localhost', 
        port : ''
        dbname : 'test'
        username : 'test'
        password : 'test'
    }

    with pymysql.connect(**config) as conn:
        cursor  = conn.cursor()
        for chunk in pd.read_csv(temp_csv_file_path, header = None, encoding = 'utf-8', sep = u'\001', chunksize= 100000):
            cursor.executemany(query, chunk.values.tolist())

        conn.commit()
    return 
```

