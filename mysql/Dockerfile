FROM mysql

ENV MYSQL_ROOT_PASSWORD pwd
ENV MYSQL_DATABASE db

COPY ./mysql/source /docker-entrypoint-initdb.d

EXPOSE 3306
