FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/taskforge ./cmd/taskforge

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
  && adduser -D -H -u 10001 taskforge

WORKDIR /app

COPY --from=build /out/taskforge /usr/local/bin/taskforge

USER taskforge

EXPOSE 8080

ENTRYPOINT ["taskforge"]
