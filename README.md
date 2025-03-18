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
