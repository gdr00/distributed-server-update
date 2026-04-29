FROM golang:1.26.2-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o server ./main

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/server .