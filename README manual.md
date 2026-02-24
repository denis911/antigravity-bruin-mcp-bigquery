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

👉 In this case, you would run:

```bash
bruin init default .
```
