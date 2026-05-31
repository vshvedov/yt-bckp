# yt-bckp — containerized for headless / homelab use.
FROM python:3.12-slim

# yt-dlp is pinned at build time. Leave blank to grab the latest on build, or
# pin an exact version:
#   docker build --build-arg YT_DLP_VERSION=2026.3.17 -t yt-bckp .
# YouTube breaks yt-dlp periodically — rebuild the image to update it.
ARG YT_DLP_VERSION=

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg gosu \
    && rm -rf /var/lib/apt/lists/* \
    && if [ -n "$YT_DLP_VERSION" ]; then \
         pip install --no-cache-dir "yt-dlp==$YT_DLP_VERSION"; \
       else \
         pip install --no-cache-dir yt-dlp; \
       fi

WORKDIR /app
COPY server.py index.html ./
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Container defaults: bind all interfaces, headless, store to a mounted volume.
ENV YT_BCKP_HOST=0.0.0.0 \
    YT_BCKP_PORT=8723 \
    YT_BCKP_DOWNLOAD_DIR=/downloads \
    YT_BCKP_OPEN_BROWSER=0 \
    YT_BCKP_YTDLP=/usr/local/bin/yt-dlp \
    PUID=1000 \
    PGID=1000

VOLUME ["/downloads"]
EXPOSE 8723

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["python3", "server.py"]
