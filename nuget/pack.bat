del .\artifacts\*.nupkg

dotnet restore ..\src\Aeron.ClientServer
dotnet pack ..\src\Aeron.ClientServer -c Release -o .\artifacts

pause