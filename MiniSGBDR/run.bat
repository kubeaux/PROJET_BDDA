@echo off
rem Création du dossier de compilation si nécessaire
if not exist out mkdir out

rem Compilation de tous les fichiers Java
javac src\*.java -d out
if errorlevel 1 exit /b

rem Exécution des tests
java -cp out TestDBConfig
java -cp out TestDiskManager
java -cp out TestBufferManager


pause
