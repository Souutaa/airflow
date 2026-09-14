Project hiện tại được dựng dựa trên:

- Apache Airflow 2.9.2
- Docker Compose
- LocalExecutor
- PostgreSQL làm Airflow metadata database
- MySQL dùng cho dữ liệu ETL/report
- Có Dockerfile custom với build: .
- Có các thư mục:
  - dags/
  - logs/
  - config/
  - plugins/
- Compose hiện tại có các service kiểu Airflow 2:
  - airflow-webserver
  - airflow-scheduler
  - airflow-init
  - airflow-cli
- Có PostgreSQL service tên postgres dành cho Airflow metadata.
- Có MySQL service.
- Có thêm một PostgreSQL business/dev service tên de_psql, nhưng hiện tại có thể không còn cần thiết.
  Hiện tại hệ thống local đã setup tương đối ổn định trên Airflow 2.9.2.
  Tôi KHÔNG muốn xoá toàn bộ project và làm lại một cách mù quáng.
  Tôi muốn:

1. Audit project hiện tại.
2. Xác định những phần có thể giữ lại.
3. Xác định breaking changes từ Airflow 2.9.2 → Airflow 3.3.1.
4. Nâng cấp từng phần một cách an toàn.
5. Sau cùng project chạy ổn định trên Airflow 3.3.1 + Python 3.11.
   Target cuối cùng:

- Python 3.11.x
- Apache Airflow 3.3.1
- Docker Compose
- LocalExecutor
- PostgreSQL 16 làm Airflow metadata DB
- MySQL 8.x làm ETL/report/business DB
- Linux container
- Không dùng CeleryExecutor
- Không dùng Redis
- Không dùng Kubernetes
- Không dùng apache/airflow:latest
- Pin rõ Airflow version.
  Airflow image mong muốn:

```dockerfile
FROM apache/airflow:3.3.1-python3.11
```

Nếu Dockerfile hiện tại cần custom provider/dependency thì giữ Dockerfile custom, nhưng base image phải được pin version.
Không được bắt đầu bằng cách overwrite toàn bộ file.
Trước tiên phải:

1. Scan repository.
2. Đọc Dockerfile.
3. Đọc docker-compose.yml / docker-compose.yaml.
4. Đọc .env và .env.example nếu có.
5. Đọc requirements.
6. Scan tất cả DAG.
7. Scan plugins.
8. Scan custom operators/hooks.
9. Kiểm tra import Airflow cũ.
10. Kiểm tra Airflow connections/variables nếu có config bằng code.
11. Kiểm tra provider packages.
12. Kiểm tra volume hiện tại.
13. Kiểm tra current container architecture.
    Sau đó cung cấp findings trước khi implementation.
    Không được đoán cấu trúc project.
    Hãy xác định chính xác:

- Version hiện tại.
- Python version hiện tại.
- Executor.
- Airflow image.
- Dockerfile custom.
- Providers đang cài.
- Các dependency custom.
  Liệt kê toàn bộ services và chức năng:

```text
service
purpose
keep/remove/change
reason
```

Đặc biệt kiểm tra:

```text
airflow-webserver
airflow-scheduler
airflow-init
airflow-cli
postgres
mysql
de_psql
```

Scan toàn bộ dags/.
Tìm các import deprecated hoặc cần migration, ví dụ:

```python
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators...
from airflow.hooks...
```

Phân loại:

```text
Compatible
Needs migration
Deprecated
Breaking in Airflow 3
```

Với code mới, ưu tiên stable public API của Airflow 3:

```python
from airflow.sdk import DAG
from airflow.sdk import dag, task
```

Nhưng KHÔNG refactor DAG chỉ để đổi syntax nếu không cần thiết.
Mục tiêu là compatibility và maintainability, không phải rewrite toàn bộ DAG.
Kiểm tra official migration requirements cho:

```text
Airflow 2.9.2
→ Airflow 3.3.1
```

Không được assume rằng có thể upgrade trực tiếp metadata database mà không kiểm tra migration path.
Xác định:

- database migrations cần chạy
- deprecated configurations
- renamed configurations
- removed configurations
- auth/API changes
- webserver → api-server changes
- DAG processor architecture
- scheduler changes
- CLI changes
- provider compatibility
- plugin compatibility
- connection compatibility
- XCom compatibility
- DAG serialization compatibility
  Nếu cần upgrade qua intermediate version hoặc chạy migration command đặc biệt, phải ghi rõ.
  Không được xoá metadata DB hiện tại để né migration trừ khi đây chỉ là disposable local environment và có lý do rõ ràng.
  Compose mới phải phù hợp với Airflow 3.3.1.
  Không giữ architecture Airflow 2 nếu Airflow 3 đã thay đổi.
  Target tối thiểu:

```text
airflow-api-server
airflow-scheduler
airflow-dag-processor
airflow-init
postgres
mysql
```

Có thể giữ airflow-cli dưới profile debug nếu thực sự hữu ích.
Không thêm Celery worker.
Không thêm Redis.
Executor:

```env
AIRFLOW__CORE__EXECUTOR=LocalExecutor
```

PostgreSQL chỉ đóng vai trò:

```text
Airflow metadata database
```

Không dùng PostgreSQL này cho business/report data.
Database connection phải được lấy từ environment variable.
Không hardcode:

```text
airflow:airflow
```

trong production-style config.
Ví dụ:

```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=...
POSTGRES_DB=airflow
```

Compose có thể sử dụng:

```yaml
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres/${POSTGRES_DB}
```

Nếu Airflow 3.3.1/provider hiện tại khuyến nghị driver PostgreSQL khác thì kiểm tra và dùng driver phù hợp.
Không tự ý đổi driver mà không kiểm tra compatibility.
Đây là REQUIREMENT bắt buộc.
Tôi KHÔNG muốn database chỉ nằm trong Docker named volume kiểu:

```yaml
postgres-db-volume:/var/lib/postgresql/data
```

hoặc:

```yaml
mysql_h:/var/lib/mysql
```

Database phải được lưu tại một đường dẫn host do tôi chỉ định.
Sử dụng bind mount.
Ví dụ .env:

```env
AIRFLOW_POSTGRES_DATA_DIR=/data/airflow/postgres
MYSQL_DATA_DIR=/data/airflow/mysql
AIRFLOW_LOG_DIR=/data/airflow/logs
```

Compose:

```yaml
postgres:
  volumes:
    - ${AIRFLOW_POSTGRES_DATA_DIR}:/var/lib/postgresql/data
```

MySQL:

```yaml
mysql:
  volumes:
    - ${MYSQL_DATA_DIR}:/var/lib/mysql
```

Mục tiêu:

```text
docker compose down
docker compose up
```

hoặc recreate container:

```text
container deleted
↓
database data vẫn tồn tại trên host
↓
container mới mount lại
↓
database tiếp tục hoạt động
```

Data directory không nên phụ thuộc vào Git repository.
Target production-style structure có thể như:

```text
/data/airflow/
├── postgres/
├── mysql/
└── logs/
```

Project:

```text
/opt/apps/airflow-project/
├── docker-compose.yml
├── Dockerfile
├── .env
├── dags/
├── plugins/
└── config/
```

Không commit database data vào Git.
Giữ MySQL nếu project đang dùng MySQL cho:

- report database
- ETL target
- development data
  Target:

```text
MySQL 8.x
```

Kiểm tra version hiện tại trước khi thay đổi.
MySQL environment phải đúng official image conventions:

```env
MYSQL_DATABASE=
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_ROOT_PASSWORD=
```

Không cần:

```env
MYSQL_ROOT_USER=root
```

Nếu dataset mount hiện tại vẫn cần:

```yaml
./dataset:/tmp/dataset
./load_dataset_into_mysql:/tmp/load_dataset
```

thì giữ.
Nếu chỉ là tutorial/demo cũ và không còn sử dụng thì đề xuất remove, nhưng không tự động remove trước khi xác nhận bằng code usage.
Hiện tại compose có thể có thêm:

```text
de_psql
```

Đây là PostgreSQL thứ hai ngoài Airflow metadata DB.
Hãy scan repository để xác định:

- DAG có connect tới de_psql không?
- script nào sử dụng?
- connection ID nào trỏ tới?
- dataset nào phụ thuộc?
- report nào phụ thuộc?
  Nếu không còn dependency:

```text
recommend: remove de_psql
```

Nhưng không remove chỉ vì thấy thừa.
Phải chứng minh nó không được sử dụng.
Không hardcode:

```text
password
database password
Airflow admin password
Fernet key
secret key
connection URL credentials
```

Sử dụng .env.
Phải có:

```text
.env
.env.example
```

.env phải nằm trong .gitignore.
.env.example chỉ chứa placeholder.
Ví dụ:

```env
POSTGRES_USER=airflow
POSTGRES_PASSWORD=change_me
POSTGRES_DB=airflow

MYSQL_DATABASE=report
MYSQL_USER=etl
MYSQL_PASSWORD=change_me
MYSQL_ROOT_PASSWORD=change_me

AIRFLOW_FERNET_KEY=change_me
```

Không commit secret thực.
Hiện tại nếu có:

```yaml
AIRFLOW__CORE__FERNET_KEY: ''
```

phải sửa.
Airflow phải sử dụng Fernet key persistent.
Ví dụ:

```env
AIRFLOW_FERNET_KEY=...
```

và:

```yaml
AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW_FERNET_KEY}
```

Không generate Fernet key mới mỗi lần container start.
Nếu metadata database hiện tại đã có encrypted Airflow connections, phải kiểm tra impact trước khi đổi Fernet key.
Không được làm mất khả năng decrypt connection cũ.
Kiểm tra cách Airflow 3.3.1 xử lý authentication và initial admin user.
Không copy nguyên:

```text
Airflow 2 webserver/auth config
```

sang Airflow 3 nếu architecture đã thay đổi.
Phải dùng configuration phù hợp Airflow 3.3.1.
Nếu current local setup đang tạo:

```text
admin/admin
```

thì có thể giữ cho local development thông qua env, nhưng phải đánh dấu rõ:

```text
DEV ONLY
```

Không dùng credential mặc định cho production.
Không dùng:

```env
_PIP_ADDITIONAL_REQUIREMENTS
```

để cài package mỗi lần container startup trong target final architecture.
Development quick-test có thể dùng tạm thời, nhưng final solution phải build dependency vào Docker image.
Ví dụ:

```dockerfile
FROM apache/airflow:3.3.1-python3.11

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt
```

Tuy nhiên phải tuân thủ Airflow constraints/version compatibility.
Không upgrade dependency tự do làm phá Airflow.
Kiểm tra các package ETL hiện tại, ví dụ:

```text
pandas
sqlalchemy
pymysql
mysqlclient
psycopg
psycopg2
pyodbc
apache-airflow-providers-mysql
apache-airflow-providers-postgres
apache-airflow-providers-microsoft-mssql
```

Chỉ giữ dependency thực sự được code sử dụng.
Xác định chính xác provider cần thiết.
Có thể cần:

```text
apache-airflow-providers-mysql
apache-airflow-providers-postgres
apache-airflow-providers-common-sql
```

Nếu project kết nối SQL Server:

```text
apache-airflow-providers-microsoft-mssql
```

Không cài tất cả provider một cách mặc định.
Pin version tương thích Airflow 3.3.1.
Không hardcode database credentials trực tiếp trong DAG.
Ví dụ không nên:

```python
mysql.connector.connect(
    host="...",
    user="...",
    password="..."
)
```

nếu đó là database connection được Airflow quản lý.
Ưu tiên Airflow Connections:

```text
source_mysql
report_mysql
source_mssql
...
```

và hook/provider tương ứng.
Nhưng không refactor một cách máy móc.
Audit code trước và đề xuất migration.
Business workload của project là:

```text
Source databases
        ↓
Extract
        ↓
Transform
        ↓
Mapping
        ↓
Validate
        ↓
Load report DB
        ↓
SQL View
        ↓
Checkpoint
```

Job chạy daily.
Airflow chỉ chịu trách nhiệm:

```text
schedule
orchestration
dependency
retry
monitoring
task state
```

Business checkpoint KHÔNG được phụ thuộc hoàn toàn vào:

```text
XCom
Airflow Variable
Airflow metadata DB
```

Checkpoint phải được persist trong business/report database.
Ví dụ:

```text
etl_checkpoint
etl_job_run
```

Không cần implement business tables này trong migration Airflow nếu project hiện tại chưa có yêu cầu, nhưng architecture phải không cản trở việc này.
Không truyền DataFrame lớn thông qua XCom.
Không làm:

```python
@task
def extract():
    return huge_dataframe
```

rồi truyền toàn bộ DataFrame sang task khác.
XCom chỉ dùng cho metadata nhỏ:

```text
checkpoint
row_count
job_id
table_name
file_path
batch_id
```

Actual dataset nên nằm ở:

```text
database
staging table
parquet/file storage
```

tùy architecture hiện tại.
Airflow logs phải persist.
Có thể bind mount:

```env
AIRFLOW_LOG_DIR=/data/airflow/logs
```

và:

```yaml
- ${AIRFLOW_LOG_DIR}:/opt/airflow/logs
```

Không để log quan trọng chỉ nằm trong ephemeral container filesystem.
Compose phải có healthcheck hợp lý cho:

```text
postgres
mysql
airflow-api-server
airflow-scheduler
airflow-dag-processor nếu có endpoint/status thích hợp
```

depends_on phải dùng health conditions khi cần.
Không sử dụng sleep cứng như:

```bash
sleep 30
```

để giải quyết startup ordering nếu có cách dùng proper healthcheck/migration.
Airflow init phải:

1. Đợi PostgreSQL healthy.
2. Chạy database migration chính xác cho Airflow 3.3.1.
3. Setup initial auth/admin nếu cần.
4. Exit success.
5. Các Airflow services chỉ start sau khi init thành công.
   Không chạy destructive reset DB.
   Không dùng:

```text
airflow db reset
```

trừ khi user chủ động yêu cầu reset local DB.
Current project có thể đang dùng Docker named volume.
Nếu chuyển:

```text
named volume
→ host bind mount
```

phải chú ý data hiện tại.
Không chỉ đổi compose rồi làm mất metadata.
Nếu local metadata cần giữ, hãy cung cấp migration procedure:

```text
existing Docker volume
↓
backup / pg_dump
↓
new host directory
↓
restore
↓
Airflow metadata migration
```

Đối với MySQL tương tự:

```text
mysqldump / physical migration phù hợp
```

Ưu tiên logical backup/restore nếu an toàn hơn.
Trước khi upgrade phải tạo checklist backup:

```text
PostgreSQL Airflow metadata
MySQL data cần giữ
.env
Docker Compose
Dockerfile
requirements
DAGs
plugins
config
```

Không thực hiện destructive changes trước khi xác định backup strategy.
Sau khi implementation phải verify thực tế.
Không kết luận "done" chỉ vì Docker build thành công.
Phải kiểm tra:

```bash
docker compose config
docker compose build
docker compose up
docker compose ps
```

Kiểm tra:

```text
Airflow version = 3.3.1
Python version = 3.11.x
Executor = LocalExecutor
PostgreSQL metadata connected
Scheduler healthy
API server healthy
Dag Processor healthy
```

Kiểm tra:

```text
DAG parsing errors = 0
```

Liệt kê DAG.
Chạy ít nhất một smoke-test DAG nếu repository có DAG phù hợp.
Nếu không có thì có thể tạo một minimal smoke DAG riêng, nhưng không được để test artifact vào production nếu không cần.
Phải verify bind mount:

1. Start PostgreSQL/MySQL.
2. Tạo hoặc xác nhận data.
3. Stop/recreate container.
4. Start lại.
5. Xác nhận data vẫn tồn tại.
   Mục tiêu phải chứng minh:

```text
container lifecycle != database lifecycle
```

Migration infrastructure và migration business DAG là hai việc khác nhau.
Ưu tiên thứ tự:

```text
1. Airflow infrastructure compatible
2. Airflow starts correctly
3. Existing DAGs parse
4. Fix incompatible DAG API
5. Run DAG smoke tests
6. Refactor code sau
```

Không refactor toàn bộ ETL cùng lúc với upgrade infrastructure nếu không bắt buộc.
Trước implementation:

```bash
git status
```

Nếu working tree dirty:

- báo rõ
- phân biệt thay đổi có sẵn của user
- không overwrite chúng
  Không:

```bash
git reset --hard
git clean -fd
```

nếu user chưa yêu cầu.
Không commit nếu user chưa yêu cầu commit.
Target tham khảo:

```text
project/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env
├── .env.example
├── .gitignore
│
├── dags/
├── plugins/
├── config/
└── scripts/
```

Host storage:

```text
/data/airflow/
├── postgres/
├── mysql/
└── logs/
```

Host paths phải configurable bằng .env, không hardcode /data/airflow trực tiếp vào tất cả environment.
Ví dụ:

```env
AIRFLOW_DATA_ROOT=/data/airflow

AIRFLOW_POSTGRES_DATA_DIR=${AIRFLOW_DATA_ROOT}/postgres
MYSQL_DATA_DIR=${AIRFLOW_DATA_ROOT}/mysql
AIRFLOW_LOG_DIR=${AIRFLOW_DATA_ROOT}/logs
```

Nếu Docker Compose .env interpolation không support nested variable theo cách mong muốn thì dùng explicit values.
Không tạo cấu hình không hoạt động chỉ để đẹp.
Thực hiện theo thứ tự.
Trước khi sửa file, trả về:

```text
Current state
Compatibility findings
Breaking changes
Files requiring changes
Files safe to keep
Unused candidates
Risks
Migration plan
```

Đặc biệt trả lời:

```text
Có thể upgrade trực tiếp từ setup hiện tại hay không?
Metadata DB migration cần làm gì?
DAG nào không tương thích?
Provider nào không tương thích?
de_psql có còn cần không?
```

Sau findings, nếu không có blocker nghiêm trọng thì tiến hành implementation.
Sửa các file cần thiết.
Ưu tiên minimal migration thay vì rewrite.
Sau implementation cung cấp:
Liệt kê từng file.
Ví dụ:

```text
docker-compose.yml
Dockerfile
requirements.txt
.env.example
.gitignore
...
```

Mô tả architecture cuối cùng.

```text
Python
Airflow
PostgreSQL
MySQL
providers
```

Xác nhận path:

```text
PostgreSQL host path
MySQL host path
Airflow log host path
```

Liệt kê command thực tế đã chạy và kết quả.
Liệt kê những thứ chưa verify được.
Nếu user cần tự tạo .env, Fernet key, directory hoặc migrate existing data thì ghi command cụ thể.
Không:

- rewrite toàn project
- reset database
- xoá volume
- xoá DAG
- xoá data
- hardcode password
- dùng latest
- đổi LocalExecutor sang Celery
- thêm Redis
- thêm Kubernetes
- truyền DataFrame lớn qua XCom
- giữ deprecated Airflow 2 architecture chỉ vì nó đang chạy
- assume compose Airflow 2 tương thích Airflow 3
- claim migration thành công mà chưa verify
  Ưu tiên:

```text
inspect
→ understand
→ backup
→ migrate infrastructure
→ migrate compatibility issues
→ verify
→ report
```

Các decision này được coi là đã chốt:

```text
Python = 3.11.x
Airflow = 3.3.1

Executor = LocalExecutor

Airflow metadata DB = PostgreSQL 16

Business/report DB = MySQL 8.x

Deployment = Docker Compose

Database persistence = host bind mounts

No Redis
No CeleryExecutor
No Kubernetes
```

Nếu trong quá trình audit phát hiện một decision trên gây incompatibility thực tế, không tự ý thay đổi.
Hãy báo:

```text
Finding
Evidence
Impact
Recommended alternative
```

rồi mới xử lý.
Bắt đầu bằng việc audit repository hiện tại và cung cấp Pre-implementation findings trước.
