del .\artifacts\*.nupkg

dotnet restore ..\src\Aeron.MediaDriver
dotnet pack ..\src\Aeron.MediaDriver -c Release -o .\artifacts

pause