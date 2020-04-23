FROM minio/mc:RELEASE.2020-04-19T19-17-53Z

COPY delta.json /delta.json
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]