Foodgram - проект для обмена рецептами между пользователями. Авторизованные пользователи
могут добавлять свои рецепты, подписываться на других авторов, добавлять их рецепты в избранное,
добавлять в покупки список ингредиентов нужных для приготовления блюда, а также скачивать этот список.

## Подготовка и запуск проекта
### Склонировать репозиторий на локальную машину:
```
git clone https://github.com/IstyxI/new_admin_panel_sprint_3.git
```
## Как запустить код:
* Установите docker:
```
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```
* Cоздайте .env файл и впишите:
    ```
    POSTGRES_DB=database
    POSTGRES_USER=postgres
    POSTGRES_PASSWORD=password
    POSTGRES_HOST=<your-docker-container-name>
    DB_PORT=5432
    ES_HOST=<your-docker-container-name>
    ES_SCHEME=http
    ES_PORT=9200
    REDIS_HOST=<your-docker-container-name>
    REDIS_PORT=6379
    WAIT_FOR_DB=True
    ```

* Зайдите в postgres_to_es и запустите docker-compose:
```
sudo docker-compose up --build
```