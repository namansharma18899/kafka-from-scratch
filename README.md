<!-- [![progress-banner](https://backend.codecrafters.io/progress/kafka/9fd3bf1e-d491-42bb-94df-9c38715e5bcc)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF) -->

["Build Your Own Kafka" Challenge]

In this challenge, you'll build a toy Kafka clone that's capable of accepting
and responding to APIVersions & Fetch API requests. You'll also learn about
encoding and decoding messages using the Kafka wire protocol. You'll also learn
about handling the network protocol, event loops, TCP sockets and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Kafka implementation is in `app/server.go`. Study and
uncomment the relevant code, and push your changes to pass the first stage:

```sh
git commit -am "pass 1st stage" # any msg
git push origin master
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `go (1.19)` installed locally
1. Run `./your_program.sh` to run your Kafka broker, which is implemented in
   `app/server.go`.
1. Commit your changes and run `git push origin master` to submit your solution
   to CodeCrafters. Test output will be streamed to your terminal.

# Testing the broker

Run the integration tests (they start an in-process broker and exercise ApiVersions, Metadata, Produce, and Fetch):

```sh
go test -v ./app/
```

# Graceful shutdown

Stop the broker with **Ctrl+C** (SIGINT) or send **SIGTERM**. The server will stop accepting new connections, wait up to 5 seconds for in-flight connections to finish, then exit.
