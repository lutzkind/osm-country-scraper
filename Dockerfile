FROM node:22-bookworm-slim

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

ENV HOST=0.0.0.0
ENV PORT=3000

RUN mkdir -p /app/data /app/data/exports

EXPOSE 3000

CMD ["node", "index.js"]
