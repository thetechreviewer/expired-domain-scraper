FROM apify/actor-node:20

COPY package*.json ./
RUN npm --quiet set progress=false \
    && npm install --omit=dev --no-audit

COPY . ./

CMD ["npm", "start"]
