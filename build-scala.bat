@echo off

if exist "%~dp0build\org\apache\spark\deploy" del %~dp0build\org\apache\scala\deploy
scalac -classpath %ASSEMBLY_JAR% %~dp0src\main\scala\NodeSparkSubmit.scala