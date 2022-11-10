FROM golang:1.19-alpine3.16 as builder
RUN apk add --no-cache git
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY ./main.go ./hpp.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags '-extldflags "-static" -s -w' -o ./hpp

FROM gcr.io/distroless/static-debian11
COPY --from=builder /build/hpp /
ENTRYPOINT [ "/hpp" ]
