FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source
COPY database/ database/
COPY src/WorkflowScheduler/WorkflowScheduler.csproj src/WorkflowScheduler/
COPY src/WorkflowScheduler.Client/WorkflowScheduler.Client.csproj src/WorkflowScheduler.Client/
COPY src/WorkflowScheduler.Worker/WorkflowScheduler.Worker.csproj src/WorkflowScheduler.Worker/
COPY src/WorkflowScheduler.Contracts.Grpc/WorkflowScheduler.Contracts.Grpc.csproj src/WorkflowScheduler.Contracts.Grpc/
COPY samples/order-demo/Directory.Build.props samples/order-demo/OrderDemo.slnx samples/order-demo/
COPY samples/order-demo/OrderDemo.Contracts/OrderDemo.Contracts.csproj samples/order-demo/OrderDemo.Contracts/
COPY samples/order-demo/OrderDemo.Inventory/OrderDemo.Inventory.csproj samples/order-demo/OrderDemo.Inventory/
COPY samples/order-demo/OrderDemo.Payments/OrderDemo.Payments.csproj samples/order-demo/OrderDemo.Payments/
COPY samples/order-demo/OrderDemo.Orders/OrderDemo.Orders.csproj samples/order-demo/OrderDemo.Orders/
COPY samples/order-demo/OrderDemo.Tests/OrderDemo.Tests.csproj samples/order-demo/OrderDemo.Tests/
RUN dotnet restore samples/order-demo/OrderDemo.slnx
COPY src/ src/
COPY samples/order-demo/ samples/order-demo/
RUN dotnet publish src/WorkflowScheduler/WorkflowScheduler.csproj -c Release --no-restore -o /out/scheduler
RUN dotnet publish samples/order-demo/OrderDemo.Inventory/OrderDemo.Inventory.csproj -c Release --no-restore -o /out/inventory
RUN dotnet publish samples/order-demo/OrderDemo.Payments/OrderDemo.Payments.csproj -c Release --no-restore -o /out/payments
RUN dotnet publish samples/order-demo/OrderDemo.Orders/OrderDemo.Orders.csproj -c Release --no-restore -o /out/orders

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
ENV ASPNETCORE_HTTP_PORTS=8080
USER $APP_UID

FROM runtime AS scheduler
COPY --from=build /out/scheduler .
ENTRYPOINT ["dotnet", "WorkflowScheduler.dll"]

FROM runtime AS inventory
COPY --from=build /out/inventory .
ENTRYPOINT ["dotnet", "OrderDemo.Inventory.dll"]

FROM runtime AS payments
COPY --from=build /out/payments .
ENTRYPOINT ["dotnet", "OrderDemo.Payments.dll"]

FROM runtime AS orders
COPY --from=build /out/orders .
ENTRYPOINT ["dotnet", "OrderDemo.Orders.dll"]
