ARG NODE_RED_VERSION=4.0.9
ARG NODE_VERSION=22
FROM nodered/node-red:${NODE_RED_VERSION}-${NODE_VERSION}

USER root

COPY package.json /opt/outbox-node/package.json
COPY lib /opt/outbox-node/lib
COPY nodes /opt/outbox-node/nodes

RUN npm install --prefix /usr/src/node-red \
      /opt/outbox-node \
      node-red-contrib-postgresql@0.15.4 \
    && mkdir -p /data/outbox \
    && chown -R node-red:root /data /opt/outbox-node /usr/src/node-red

COPY --chown=node-red:root demo/flows.json /data/flows.json
COPY --chown=node-red:root demo/settings.js /data/settings.js

USER node-red
