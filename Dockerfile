FROM golang:1.20 AS builder
WORKDIR /build
ADD go.mod go.sum /build/
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o spark-submit-server .

FROM apache/spark:3.5.0
WORKDIR /spark/home
COPY --from=builder /build/spark-submit-server .
USER 185
ENTRYPOINT []
CMD [ "./spark-submit-server" ]
