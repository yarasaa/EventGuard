# Build with only the published .NET 10 load-tool output as the context.
# Keep the EventGuard.LoadTests assembly name; the wrapper invokes that DLL.
FROM mcr.microsoft.com/dotnet/aspnet:10.0.12-noble@sha256:6a94333d37514e385650a3c81a55e5350b67253dbe136e9cf17e499c35606a8c
# Npgsql/.NET negotiate GSSAPI support when opening PostgreSQL connections.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN chgrp -R 0 /app && chmod -R g=u /app
ENV DOTNET_EnableDiagnostics=0
USER 10001:0
CMD ["sleep", "infinity"]
