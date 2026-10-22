FROM mcr.microsoft.com/dotnet/sdk:10.0.402-noble AS build
WORKDIR /source
COPY src/WorkflowScheduler/WorkflowScheduler.csproj src/WorkflowScheduler/
COPY src/WorkflowScheduler.Contracts/WorkflowScheduler.Contracts.csproj src/WorkflowScheduler.Contracts/
RUN dotnet restore src/WorkflowScheduler/WorkflowScheduler.csproj
COPY src/WorkflowScheduler/ src/WorkflowScheduler/
COPY src/WorkflowScheduler.Contracts/ src/WorkflowScheduler.Contracts/
COPY database/ database/
RUN dotnet publish src/WorkflowScheduler/WorkflowScheduler.csproj -c Release --no-restore -o /out /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0.13-noble
WORKDIR /app
ENV ASPNETCORE_ENVIRONMENT=Production \
    Kestrel__Endpoints__Grpc__Url=http://0.0.0.0:8080 \
    Kestrel__Endpoints__Grpc__Protocols=Http2 \
    Kestrel__Endpoints__Health__Url=http://0.0.0.0:8081 \
    Kestrel__Endpoints__Health__Protocols=Http1
EXPOSE 8080 8081
USER $APP_UID
COPY --from=build /out .
ENTRYPOINT ["dotnet", "WorkflowScheduler.dll"]
