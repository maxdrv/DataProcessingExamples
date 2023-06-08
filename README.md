# DataProcessingExamples


Before starting the application locally, you need to start the database
`cd dependency-stub && docker-compose up -d`


dbQueue запускается вместе со стартом приложения благодаря
DbQueueApplicationStartListener

    
Если это spring-boot приложение выключить с помощью SIGINT (signal 2)
То DbQueue pool отключится благодаря shutdown методу у QueueService

QueueService.shutdown вызывает ionShutdownHook

А так же ionShutdownHook вызывает shutdown для HikariPool

Как работает ionShutdownHook?
Кто регистрирует, что нужно вызвать shutdown?
Что происходит, если выполняются задачи в моменте? Закончится ли выполнение задач? 
Если не закончится? Но они уже взяты в работу с точки зрения записанных в базу данных?