# Build context: repository root  (docker build -f src/Shard/Dockerfile .)
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source

COPY Directory.Build.props ./
COPY src/Shared/Shared.csproj src/Shared/
COPY src/Shard/Shard.csproj src/Shard/
RUN dotnet restore src/Shard/Shard.csproj

COPY src/Shared/ src/Shared/
COPY src/Shard/ src/Shard/
RUN dotnet publish src/Shard/Shard.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
RUN mkdir -p /app/data && chown -R $APP_UID /app/data
COPY --from=build /app ./

ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
VOLUME ["/app/data"]
USER $APP_UID
ENTRYPOINT ["dotnet", "ShardedKvStore.Shard.dll"]
