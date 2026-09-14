# Build-data-warehouse-with-Airflow-Python-for-E-commerce
🚀 Just completed an exciting ELT data pipeline project focused on eCommerce data! This end-to-end pipeline extracts sales data, stages it for further analysis, and transforms it into actionable insights in a data warehouse.

Key languages and data platforms of the project:

Python and custom scripts were used to extract and load eCommerce data efficiently from CSV files and MySQL databases.
PostgreSQL served as the staging area and the final data warehouse, handling structured sales data.
Apache Airflow orchestrated the entire workflow, ensuring smooth operations from extraction to transformation.
Containerized the project using Docker for seamless environment management and consistent deployment.
Power BI provided the final layer of visualization, offering insightful sales analytics and business intelligence.
![image](https://github.com/user-attachments/assets/6eaf6963-8272-437a-9bdf-1b57bfc538e2)


![image](https://github.com/user-attachments/assets/45bd11be-7501-4d12-b33a-47399a763518)

### Dashboard with Power BI
![image](https://github.com/user-attachments/assets/5be72d62-6aa3-44c5-ab8d-6001057d7434)

![image](https://github.com/user-attachments/assets/0527464a-c116-4582-8add-ab256ba685da)

## Deployment

The project uses one `docker-compose.yaml` for every environment. Airflow runs
in Docker and connects to PostgreSQL/MySQL through their real private IP or DNS
names; database containers are not part of this Compose project.

Select the environment only through its env file:

```bash
# Development
docker compose --env-file .env.dev up -d --wait

# Production
docker compose --env-file .env.prod up -d --wait
```

`.env.dev` and `.env.prod` must contain identical keys and differ only in
values. Both files are ignored by Git. Use `.env.example` as the canonical
schema and see `deployment/server/README.md` for database bootstrap, migration,
verification, and rollback instructions.

External databases use dynamic `AIRFLOW_CONNECTION_<ID>_*` component fields.
Adding another database only requires adding its ID and fields to both env
files; `docker-compose.yaml` remains unchanged.
