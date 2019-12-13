FROM minio/mc:RELEASE.2019-10-09T22-54-57Z

COPY delta.json /delta.json
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]