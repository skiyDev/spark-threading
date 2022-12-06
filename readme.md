***Руководство по работе с решением***

Требования:
* система сборки sbt 1.3.0 или выше

В решение реализовано два сценария запуска:
* com.axenix.pro.SingleThreading - однопоточный запуск
* com.axenix.pro.MultiThreading - многопоточный запуск

Логика инкапсулирована в классе com.axenix.pro.Common

Класс com.axenix.pro.ArgProperties отвечает за хранение параметров запуска сценариев

Данные размещены в каталоге **/data** в корне проекта и содержат два источника данных:
* movies.csv - набор фильмов с идентификатором, название и описание жанров
* ratings.csv - содержит для каждого пользователя оценку для каждого фильма

Запуск сценариев можно выполнить из корневой папки проекта с помощью следующих команд:
* spark-submit --class com.axenix.pro.SingleThreading .\scripts\spark-threading_2.12-0.1.0.jar .\data\movies.csv .\data\result\genres .\data\ratings.csv .\data\result\avg_ratings .\data\result\statistics
* spark-submit --class com.axenix.pro.MultiThreading .\scripts\spark-threading_2.12-0.1.0.jar .\data\movies.csv .\data\result\genres .\data\ratings.csv .\data\result\avg_ratings .\data\result\statistics

Для выполнения юнит-тестов в среде Windows могут понадобиться исполняемый файл winutils.exe и библиотека hadoop.dll
Найти необходимые бинарные файлы можно по ссылке https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1/bin

Разместить их можно в любой директории для которой нужно создать переменную HADOOP_HOME и далее добавить её в список PATH