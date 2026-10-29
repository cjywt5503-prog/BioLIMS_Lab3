# BioLIMS Лабораторна робота 3

Повні файли для демонстрації Transactional Outbox, Transactional Inbox та SAGA
з компенсацією через Dead Letter Queue. Зовнішні маршрути `/api/orders`,
`/api/assays/run`, `/api/assays/outbox` і `/api/reports` збережено.

## Заміна та запуск

1. Зупинити старий проєкт командою `docker compose down` у його папці.
   Параметр `-v` не потрібний: він видаляє томи з даними.
2. Замінити всю папку `BioLIMS_Lab3` файлами з архіву. Папка `common` обов'язкова.
   Оновлено три `app.py`, три `Dockerfile` та `docker-compose.yml`.
3. Відкрити термінал у папці з `docker-compose.yml` і виконати:

```sh
docker compose up -d --build
docker compose ps
```

Потрібен Docker із Compose v2. Якщо попередні роботи досі працюють, їхні контейнери
можуть займати порти 8080, 8500, 5672 та 15672 — спершу зупинити відповідний проєкт.
Не потрібно видаляти старі БД чи очищувати старі черги.

Для перевірки реплік на одному Docker-хості:

```sh
docker compose up -d --build --scale order-sample-service=3 --scale assay-execution-service=2 --scale validation-report-service=2
```

Репліки одного сервісу використовують один і той самий том його SQLite.
Intake, Assay і Report мають різні БД й різні томи. Нові файли БД та томи позначені
`v2`, щоб не змішувати змінену схему зі старою. Попередні навчальні дані автоматично
не мігруються. На старті створюються два явні замовлення: `CODE128-1001` та
`CODE128-ERROR`. Інші невідомі штрихкоди повертають 404.

- API Gateway: http://localhost:8080/api/orders
- Consul: http://localhost:8500
- RabbitMQ: http://localhost:15672 — користувач `biolims`, пароль `biolims_demo`.

Це навчальні облікові дані локального стенда.

## Перевірка обох сценаріїв

За наявності Python 3.11+ на комп'ютері:

```sh
python smoke_test.py
```

Скрипт використовує лише стандартну бібліотеку. Він створює нові замовлення на GLU,
перевіряє завершення успішної SAGA, відкат помилкової SAGA, відсутність звіту після
відкату, збереження статусу компенсації та відповіді 404/409. Перший запуск може
чекати готовності Consul і споживачів RabbitMQ.

Ручні команди для PowerShell:

```powershell
$ok = @{ barcode="CODE128-1001"; initial_message="Start SAGA OK" } | ConvertTo-Json
$success = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/assays/run -ContentType application/json -Body $ok
$success | ConvertTo-Json -Depth 8

$bad = @{ barcode="CODE128-ERROR"; initial_message="Start SAGA FAIL" } | ConvertTo-Json
$failure = Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/assays/run -ContentType application/json -Body $bad
$failure | ConvertTo-Json -Depth 8

Start-Sleep -Seconds 5
Invoke-RestMethod "http://localhost:8080/api/assays/$($success.assay_id)" | ConvertTo-Json -Depth 8
Invoke-RestMethod "http://localhost:8080/api/assays/$($failure.assay_id)" | ConvertTo-Json -Depth 8
Invoke-RestMethod http://localhost:8080/api/assays/outbox | ConvertTo-Json -Depth 8
Invoke-RestMethod http://localhost:8080/api/reports | ConvertTo-Json -Depth 10
```

П'ять секунд — орієнтовна пауза, а не гарантія готовності. Якщо стан ще
`PENDING_VALIDATION`, повторити GET; `smoke_test.py` сам опитує до кінцевого стану.
Для коректного відображення кирилиці зручно користуватися PowerShell 7.

Очікувані результати:

| Сценарій | AssayService | ReportService |
|---|---|---|
| Успіх | `COMPLETED_SUCCESSFULLY` | Звіт у SQLite, Inbox `PROCESSED`, подія успіху в Outbox |
| Помилка | `ROLLED_BACK_BY_SAGA_DLQ`; Outbox `COMPENSATED_FROM_DLQ` | Звіту немає, Inbox `REJECTED`, події успіху немає |

Кожен POST створює новий аналіз і новий `message_id`. Inbox відсіює повторну доставку
тієї самої події, а не повторні незалежні HTTP-запити.

## Як працює реалізація

1. OrderService перевіряє існування замовлення. AssayService вибирає здорову репліку
   з Consul за Round Robin і бере лише призначені біомаркери.
2. В одній транзакції SQLite продьюсер записує аналіз зі станом
   `PENDING_VALIDATION` і повідомлення `AssayCompletedEvent` у Outbox.
3. Relay короткою транзакцією резервує запис на 90 секунд, а потім публікує його
   поза транзакцією БД. Використано publisher confirms, persistent-повідомлення та
   `mandatory=True`. Статус `SENT_TO_BROKER` встановлюється після підтвердження.
4. Консьюмер в одній транзакції записує звіт, Inbox `PROCESSED` і подію
   `AssayValidatedEvent` у власний Outbox. ACK надсилається тільки після COMMIT.
5. Relay консьюмера доставляє подію успіху. Продьюсер атомарно записує її Inbox і
   встановлює `COMPLETED_SUCCESSFULLY`.
6. Для `simulate_failure=true` або штрихкоду з `ERROR`/`FAIL` виняток виникає після
   INSERT звіту, але до COMMIT. Відкочуються всі зміни цієї транзакції. Окремо
   зберігається рішення `REJECTED`, а `basic_reject(requeue=False)` направляє подію
   через DLX у DLQ. Продьюсер атомарно компенсує аналіз і запам'ятовує обробку DLQ.

Тимчасова помилка SQLite не призводить до ACK чи бізнес-компенсації: з'єднання
споживача закривається, а непідтверджена подія повторюється після перепідключення.
Повторний успіх/відкат ідемпотентний. Конфліктні кінцеві стани не перезаписуються.
Якщо аналізу немає у БД продьюсера, компенсація не підтверджується як успішна.

Після аварії relay невідправлений запис повторно доступний після закінчення
90-секундної оренди. Втрата підтвердження може спричинити дубль, який відсіює Inbox.
Паралельні relay використовують окремі з'єднання SQLite та взаємно виключні
резервування; оновлення Outbox не перезаписує вже виконану компенсацію.

## Черги RabbitMQ

Використано окремі імена для нового контракту, щоб уникнути конфлікту властивостей
із чергами попередньої роботи:

- `assay_completed_queue_v2` — події аналізів;
- `assay_dlx_v2` — обмінник відхилених подій;
- `assay_dlq_queue_v2` — компенсації SAGA;
- `assay_validated_queue_v2` — підтвердження успішної валідації.

Черги quorum, основна черга має `x-dead-letter-strategy=at-least-once` та
`x-overflow=reject-publish`: RabbitMQ утримує відхилену подію до підтвердження
доставки в DLQ. Версія RabbitMQ закріплена на 3.13.7. Один брокер у Compose
не забезпечує високу доступність кластера; том зберігає його дані при перестворенні
контейнера. Старі черги автоматично не очищуються й не споживаються новими сервісами.

## Додаткові демонстрації для звіту

**Недоступний брокер:** виконати `docker compose stop rabbitmq`, створити аналіз,
відкрити `/api/assays/outbox` і показати `PENDING`. Після
`docker compose start rabbitmq` дочекатися доставки й кінцевого стану SAGA.

**Збереження звітів:** після успішного аналізу виконати
`docker compose restart validation-report-service`, потім повторно відкрити
`/api/reports`. Звіт залишається в SQLite. Для перевірки перестворення контейнерів
можна виконати `docker compose down`, потім `docker compose up -d`; не додавати `-v`.

**Скріншоти:** зробити нові знімки запуску обох сценаріїв, кінцевих станів
`/api/assays/outbox`, `/api/reports` і нових черг RabbitMQ. Старі знімки більше не
відповідають оновленим іменам черг і вмісту Inbox.

## Тести та межі перевірки

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-test.txt
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

У Linux/macOS замість `.\.venv\Scripts\python.exe` використати `.venv/bin/python`.

19 компонентних тестів пройдені на Python 3.12. Вони використовують реальні FastAPI
та SQLite і тестову підміну HTTP/AMQP. Перевірено обидві гілки SAGA, атомарність
продьюсера та консьюмера, збереження після переініціалізації, конкурентні дублікати,
втрату підтвердження, відновлення оренди Outbox, виконання компенсації іншою реплікою
та гонку між relay і компенсацією. Повний Docker-стек у середовищі підготовки не
запускався; для цього надано `smoke_test.py`. Docker-образ сервісів використовує
Python 3.11.

SQLite зі спільними локальними томами — спрощення для одного Docker-хоста, а не
реалізація PostgreSQL із початкової архітектури. Для різних серверів потрібна
мережева БД. Надійність припускає збереження томів і відновлення доступності
брокера/БД; видалення даних адміністратором не компенсується патернами.

Фотометричні значення фіксовані (`simulation=true`). DeltaCheck і КЕП не реалізовані,
тому звіт повертає `NOT_IMPLEMENTED` та `NOT_SIGNED`. Неправильні або конфліктні
події журналюються й відхиляються; між власними сервісами використовуються
перевірені контракти v2.
