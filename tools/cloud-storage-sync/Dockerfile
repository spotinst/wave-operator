FROM python:3.9

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN pip install requests flask gunicorn

RUN curl -O https://downloads.rclone.org/rclone-current-linux-amd64.zip
RUN unzip rclone-current-linux-amd64.zip

RUN cp rclone-*-linux-amd64/rclone /usr/bin/
RUN chown root:root /usr/bin/rclone
RUN chmod 755 /usr/bin/rclone

WORKDIR /opt/rclone

COPY rclone.conf rclone.conf
COPY const.py const.py
COPY sync.py sync.py
COPY server.py server.py
COPY run.sh run.sh

RUN chmod +x run.sh
ENV RCLONE_CONFIG /opt/rclone/rclone.conf

ENTRYPOINT [ "/tini", "--", "python", "sync.py" ]
