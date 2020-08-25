@echo off

dir .\artifacts

setlocal
:PROMPT
SET /P AREYOUSURE=Push to NuGet.org? (Y/[N])?
IF /I "%AREYOUSURE%" NEQ "Y" GOTO END

@for %%f in (.\artifacts\*.nupkg) do dotnet nuget push %%f --source https://www.nuget.org/api/v2/package --timeout 10000

:END
endlocal

pause