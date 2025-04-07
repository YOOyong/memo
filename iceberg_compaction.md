- catalog 확인
  - spark ? or hive ?
 

Iceberg에서는 Spark SQL로 아래처럼 compaction을 수행.  

```sql
CALL catalog.db.table.rewrite_data_files();
```
이 명령은 내부적으로 다음과 같은 일을 함:  
1. 작은 data file들을 합쳐서 더 큰 파일로 rewrite 
2. 기존 snapshot을 기반으로 새로운 snapshot + metadata.json 생성 
3.catalog (여기선 Hive) 에 새로운 metadata.location 갱신 (카탈로그 구현체에 따라 다름)

이때, hive catalog 일 경우 
Hive의 TBLPROPERTIES에 metadata location을 직접 갱신한다  

### 문제가 발생하는 경우, 의심해야 할 상
- SQL CALL로 생성된 새로운 metadata.json이 Hive에 정상 등록되지 않았거나, 지워졌거나
- Spark session 이 metadata cache를 유지하고 있음 (→ 새로운 insert 시 오류 발생 가능)
- rewrite_data_files 명령 직후 .expire_snapshots() 등으로 바로 metadata 파일 삭제


### 해결 방안
- pyspark 코드에서 refresh 수행
```python
spark.sql("CALL catalog.db.table.rewrite_data_files()")
spark.catalog.refreshTable("catalog.db.table")
```

- compact 직후에 수행하는 expire는 위험할 수 있다
```python
# 위험: compact 직후 snapshot 만료하면 아직 Impala가 metadata를 안 봤는데 지워질 수 있음
spark.sql("CALL catalog.db.table.expire_snapshots(retention_threshold => '1d')")
```

### 실무 운영 예시
```python
# 1. Compact
spark.sql("CALL catalog.db.table.rewrite_data_files()")

# 2. Refresh Spark 메타 (다음 insert 대비)
spark.catalog.refreshTable("catalog.db.table")

# 3. Impala invalidate
# (이건 외부 스크립트에서 Impala shell로)
# shell> impala-shell -q "INVALIDATE METADATA db.table"

# 4. (선택) snapshot expire → 일정 delay 후 백그라운드에서 수행
# spark.sql("CALL catalog.db.table.expire_snapshots(retention_threshold => '1d')")
# 하루 이상 지연을 주는 것이 일반적이다.
```

