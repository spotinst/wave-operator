FROM python:3.9

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN pip install pyinotify requests

RUN curl -O https://downloads.rclone.org/rclone-current-linux-amd64.zip
RUN unzip rclone-current-linux-amd64.zip

RUN cp rclone-*-linux-amd64/rclone /usr/bin/
RUN chown root:root /usr/bin/rclone
RUN chmod 755 /usr/bin/rclone

WORKDIR /home/rclone/
RUN groupadd -g 1500 -r rclone && useradd --no-log-init -u 1501 -r -g rclone rclone

COPY rclone.conf rclone.conf
COPY sync.py sync.py

RUN chown -R 1501 /home/rclone/

ENV RCLONE_CONFIG /home/rclone/rclone.conf 
USER rclone
ENTRYPOINT [ "/tini", "--", "python", "sync.py" ]