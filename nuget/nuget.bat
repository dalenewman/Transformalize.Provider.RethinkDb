nuget pack Transformalize.Provider.RethinkDb.nuspec -OutputDirectory "c:\temp\modules"
nuget pack Transformalize.Provider.RethinkDb.Autofac.nuspec -OutputDirectory "c:\temp\modules"

REM nuget push "c:\temp\modules\Transformalize.Provider.RethinkDb.0.6.20-beta.nupkg" -source https://api.nuget.org/v3/index.json
REM nuget push "c:\temp\modules\Transformalize.Provider.RethinkDb.Autofac.0.6.20-beta.nupkg" -source https://api.nuget.org/v3/index.json