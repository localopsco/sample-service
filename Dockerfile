FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 3000

# Numeric UID, not `USER node`. The kubelet's runAsNonRoot check reads the image
# config's User string and rejects anything it cannot parse as an integer, so
# `USER node` is refused at startup ("cannot verify user is non-root") even
# though it is a genuinely non-root account. 1000 is node:alpine's `node` user.
USER 1000:1000

ENTRYPOINT ["node"]
CMD ["web.js"]
