[![Python](https://img.shields.io/badge/-Python-464646?style=flat-square&logo=Python)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/-PostgreSQL-464646?style=flat-square&logo=PostgreSQL)](https://www.postgresql.org/)
[![docker](https://img.shields.io/badge/-Docker-464646?style=flat-square&logo=docker)](https://www.docker.com/)
[![Redis](https://img.shields.io/badge/Redis-%23DD0031.svg?logo=redis&logoColor=white)](#)
## О сервисе:
Сервис представляет собой реализацию ETL процесса. Данные берутся из PostgreSQL и переносятся в Elastic search. Redis используется для сохранения состояния при переносе данных.
## Как запустить код:
* Скопировать код:
```
git clone https://github.com/IstyxI/new_admin_panel_sprint_3.git
```

* Для запуска требуется docker-compose

* Cоздайте .env файл и впишите ваши переменные опираясь на postgres_to_es/.env.example

* Зайдите в postgres_to_es и запустите docker-compose:
```
sudo docker-compose up --build
```
