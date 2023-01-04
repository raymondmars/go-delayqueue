FROM alpine:3.17
RUN apk --no-cache add ca-certificates
RUN apk update && apk add tzdata

#
# FROM scratch
WORKDIR /root/

COPY ./go-delayqueue ./

EXPOSE 3450

ENTRYPOINT ["./go-delayqueue"]
