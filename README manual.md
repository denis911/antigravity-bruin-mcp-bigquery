# antigravity-bruin-mcp-bigquery
Using antigravity with bruin, bruin mcp and bigquery for NY taxi data pipeline

For GIT BASH use 
```Terminal: Select Default Profile```
and select `Git Bash`

For running uv use 
```bash
uv init
uv venv
uv sync
```

For duckdb use 
```bash
uv add duckdb
```

For bruin use 
```bash
uv add bruin
```

and init bruin project as follows:
```bash
bruin init
```

NB! Structure for single Bruin project per repo (most common):

```
repo-root/
├── bruin.yaml
├── pipelines/
└── ...
```

👉 In this case, you would start with:

```bash
bruin init default .
```

then check with:
```bash
bruin validate .
``` 

and finally run with:
```bash
bruin run .
``` 

As a result a new duckdb flie is created - `duckdb.db`
Run the following to check the contents of the duckdb file:
```bash
duckdb -ui duckdb.db
``` 

and see what is inside:

```sql
SELECT name, count(*) AS player_count
FROM dataset.players
GROUP BY 1
```
