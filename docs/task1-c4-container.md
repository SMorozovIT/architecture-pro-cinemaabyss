# Задание 1. To Be архитектура CinemaAbyss

## Контейнерная диаграмма C4

```mermaid
C4Container
    title To Be контейнерная архитектура CinemaAbyss

    Person(customer, "Пользователь", "Смотрит каталог, управляет подпиской и платежами")
    Person(admin, "Администратор", "Управляет каталогом фильмов и контролирует работу платформы")

    System_Boundary(cinemaabyss, "CinemaAbyss") {
        Container(apiGateway, "Proxy Service / API Gateway", "Go, HTTP", "Единая точка входа в систему. Маршрутизирует запросы по доменам, поддерживает Strangler Fig и процентное переключение трафика.")

        Container(monolith, "Legacy Monolith", "Go, HTTP", "Временно сохраняет домены пользователей, платежей и подписок, а также часть функций каталога до завершения миграции.")
        Container(moviesService, "Movies Service", "Go, HTTP", "Домен фильмов: метаданные, жанры, рейтинги, операции с каталогом.")
        Container(eventsService, "Events Service", "Go, HTTP, Kafka Producer/Consumer", "Интеграционный домен событий. Принимает события User/Movie/Payment, публикует их в Kafka и обрабатывает сообщения.")

        ContainerDb(postgres, "PostgreSQL", "Relational DB", "Единое хранилище данных переходного периода: users, movies, payments, subscriptions, views, ratings.")
        ContainerQueue(kafka, "Kafka", "Message Broker", "Асинхронная интеграция доменов через топики movie-events, user-events, payment-events.")
        Container(kafkaUi, "Kafka UI", "Web UI", "Операционный просмотр топиков, сообщений и состояния Kafka.")
    }

    Rel(customer, apiGateway, "Вызывает API", "HTTPS/HTTP")
    Rel(admin, apiGateway, "Администрирует каталог и данные", "HTTPS/HTTP")

    Rel(apiGateway, moviesService, "Маршрутизирует /api/movies с фиче-флагом миграции", "HTTP")
    Rel(apiGateway, eventsService, "Маршрутизирует /api/events/*", "HTTP")
    Rel(apiGateway, monolith, "Маршрутизирует legacy-домены /api/users, /api/payments, /api/subscriptions и остаточный трафик movies", "HTTP")

    Rel(monolith, postgres, "Читает и изменяет пользователей, платежи, подписки и legacy-данные фильмов", "SQL")
    Rel(moviesService, postgres, "Читает и изменяет фильмы, жанры и рейтинги", "SQL")

    Rel(eventsService, kafka, "Публикует и читает доменные события", "Kafka protocol")
    Rel(kafkaUi, kafka, "Отображает топики и сообщения", "Kafka protocol")

    UpdateRelStyle(customer, apiGateway, $textColor="black", $lineColor="black")
    UpdateRelStyle(admin, apiGateway, $textColor="black", $lineColor="black")
```

## Домены и ответственность

- **API Gateway / Proxy**: единая точка вызова всех сервисов, маршрутизация по доменным API, фасад для клиентов, постепенное переключение трафика с монолита на микросервисы.
- **Movies**: каталог фильмов, метаданные, жанры, рейтинги и будущая изоляция всех операций с фильмами.
- **Users**: пользователи и регистрационные данные. На текущем этапе остается в монолите как отдельный домен-кандидат на выделение.
- **Payments**: платежи и статусы транзакций. На текущем этапе остается в монолите, но публикует/потребляет события через интеграционный контур.
- **Subscriptions**: подписочные планы, сроки действия и автопродление. На текущем этапе остается в монолите.
- **Events / Integration**: асинхронное взаимодействие между доменами через Kafka, публикация и обработка событий `movie-events`, `user-events`, `payment-events`.

## Интеграционное взаимодействие

- Синхронные клиентские запросы проходят только через **Proxy Service / API Gateway**.
- Вызовы `/api/movies` постепенно переводятся на **Movies Service** с помощью фиче-флага и процента миграции.
- Legacy-домены до выделения обслуживаются **Monolith**, но остаются явно разделенными по API и данным.
- События доменов передаются через **Events Service** и **Kafka**, чтобы новые сервисы могли интегрироваться асинхронно без жесткой связности.
