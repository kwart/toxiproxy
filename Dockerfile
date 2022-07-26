FROM scratch

EXPOSE 8474
ENTRYPOINT ["/toxiproxy"]
CMD ["-host=0.0.0.0"]

ENV LOG_LEVEL=info

COPY dist/toxiproxy-server /toxiproxy
COPY dist/toxiproxy-cli /toxiproxy-cli
