# Deploy the pipeline to a single URL

The app serves:
- **GET /** — Info page with links
- **GET /run** — Runs the pre_filter pipeline and returns JSON summary (pass/fail/review counts)
- **GET /output** — Downloads the last `filtered_output.csv`

## Prerequisites on the server

- **Node.js** 18+ (for `npm start`)
- **Python 3** with: `pip install -r requirements.txt`
- These files in the project root: `pre_filter.py`, `config.yaml`, and an input CSV (default: `raw_tam.csv`)

## Option 1: Railway

1. Push this repo to GitHub (or connect existing repo).
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub.
3. Select this repo. Railway will detect Node and run `npm start`.
4. Add a **Nixpacks** or **Docker** setup so Python 3 and `pip install -r requirements.txt` run before start (or use a **Dockerfile** that installs Node + Python and runs `npm start`).
5. Set env vars if needed: `INPUT_CSV`, `OUTPUT_CSV`, `CONFIG_PATH`, `PORT`.
6. Deploy. Your URL: `https://<your-app>.up.railway.app` → visit `/run` to run the pipeline.

**Simple Dockerfile (optional)** so Railway runs both Node and Python:

```dockerfile
FROM node:20-bookworm
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY requirements.txt pre_filter.py config.yaml ./
COPY raw_tam.csv* ./
RUN pip3 install -r requirements.txt
COPY server.js ./
EXPOSE 3000
ENV PORT=3000
CMD ["node", "server.js"]
```

Then in Railway, set **Root Directory** if needed and use **Dockerfile** as build.

## Option 2: Render

1. Push to GitHub. Go to [render.com](https://render.com) → New → Web Service.
2. Connect the repo. Build: `npm install`. Start: `npm start`.
3. Render uses a single machine; install Python in a **Build Command**: e.g. `pip install -r requirements.txt && npm install`.
4. Ensure `pre_filter.py`, `config.yaml`, and `raw_tam.csv` are in the repo (or provide them via env/secrets).
5. Deploy. URL: `https://<your-app>.onrender.com/run`.

## Option 3: Run locally

```bash
npm start
# Open http://localhost:3000/run
```

## Environment variables

| Variable       | Default           | Description                    |
|----------------|-------------------|--------------------------------|
| `PORT`         | `3000`            | Server port                    |
| `INPUT_CSV`    | `raw_tam.csv`     | Path to input CSV              |
| `OUTPUT_CSV`   | `filtered_output.csv` | Path to output CSV         |
| `CONFIG_PATH`  | `config.yaml`     | Pipeline config (optional; use `--layer1-only` if missing) |
