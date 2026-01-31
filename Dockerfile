# Build stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /app

# Restore
COPY matrix-indexer.csproj .
RUN dotnet restore -r linux-x64

# Copy source and publish
COPY Program.cs .
RUN dotnet publish -c Release -r linux-x64 -o /out --no-restore -p:PublishSingleFile=true -p:PublishTrimmed=false

# Output stage (just to copy the binary out)
FROM scratch AS export
COPY --from=build /out/matrix-indexer /matrix-indexer

# Runtime stage (if you want to run it in docker)
FROM mcr.microsoft.com/dotnet/runtime-deps:8.0 AS runtime
WORKDIR /app
COPY --from=build /out/matrix-indexer .
CMD ["./matrix-indexer"]
