# Stage 1: Build the Go application
FROM golang:latest as builder
WORKDIR /app
COPY invoke.go ./
COPY script.sh ./
# Initialize Go module and build the server binary
RUN go mod init simplepoc && go mod tidy
# Build the server executable for Linux
RUN CGO_ENABLED=0 GOOS=linux go build -v -o server

# Stage 2: Create the dbt execution environment
FROM ghcr.io/dbt-labs/dbt-bigquery:latest
WORKDIR /dbt
# Copy the compiled Go server binary and the execution script
COPY --from=builder /app/server ./
COPY script.sh ./

# Copy the dbt project files
COPY profiles.yml ./
COPY dbt_project.yml ./
COPY models/ models/

# Install dbt dependencies (if any packages are used)
RUN dbt deps --profiles-dir .

# Set the Go server as the entry point
ENTRYPOINT ["./server"]