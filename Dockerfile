FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/qrunnr ./cmd/qrunnr

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
  && adduser -D -H -u 10001 qrunnr

WORKDIR /app

COPY --from=build /out/qrunnr /usr/local/bin/qrunnr

USER qrunnr

EXPOSE 8080

ENTRYPOINT ["qrunnr"]
