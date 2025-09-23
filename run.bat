@echo off
rem Compilation
if not exist out mkdir out
javac src\*.java -d out

rem Exécution
java -cp out TestDBConfig
