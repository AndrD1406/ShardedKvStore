# Build context: repository root  (docker build -f src/Coordinator/Dockerfile .)
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source

COPY Directory.Build.props ./
COPY src/Shared/Shared.csproj src/Shared/
COPY src/Coordinator/Coordinator.csproj src/Coordinator/
RUN dotnet restore src/Coordinator/Coordinator.csproj

COPY src/Shared/ src/Shared/
COPY src/Coordinator/ src/Coordinator/
RUN dotnet publish src/Coordinator/Coordinator.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
RUN mkdir -p /app/data && chown -R $APP_UID /app/data
COPY --from=build /app ./

ENV ASPNETCORE_HTTP_PORTS=8080 \
    DATA_DIR=/app/data
EXPOSE 8080
VOLUME ["/app/data"]
USER $APP_UID
ENTRYPOINT ["dotnet", "ShardedKvStore.Coordinator.dll"]
