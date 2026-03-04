# Single-URL pipeline: Node server + Python script
FROM node:20-bookworm

RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY requirements.txt pre_filter.py config.yaml server.js ./
# Include input CSV in the image (or override INPUT_CSV at runtime)
COPY raw_tam.csv ./

RUN pip3 install --break-system-packages -r requirements.txt || pip3 install -r requirements.txt

EXPOSE 3000
ENV PORT=3000
CMD ["node", "server.js"]
