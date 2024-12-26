# syntax=docker/dockerfile:1.4
FROM --platform=$BUILDPLATFORM node:22-alpine AS development

RUN mkdir /project
WORKDIR /project

COPY . .

RUN yarn add -D vite @vitejs/plugin-vue
RUN yarn install
#RUN yarn create vite app --template vue
ENV HOST=0.0.0.0
CMD ["yarn", "vite"]

FROM development as dev-envs
RUN <<EOF
apk update
apk add git
EOF

RUN <<EOF
addgroup -S docker
adduser -S --shell /bin/bash --ingroup docker vscode
EOF
# install Docker tools (cli, buildx, compose)
COPY --from=gloursdocker/docker / /
CMD ["yarn", "vite"]
