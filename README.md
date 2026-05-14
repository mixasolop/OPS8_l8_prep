Predicted lab task: Broadcast server
L7: Broadway Broadcast

Broadway is preparing a live announcement system. There is one person on stage — the speaker/source — and many people in the audience — the clients/listeners.

Your task is to write a single-process server using UNIX stream sockets and epoll. The server accepts many clients. One connected client can become the source of messages. Every complete message sent by the source must be forwarded to all ordinary clients.

All complete messages end with \n. No message, including the final \n, is longer than MAX_MSG_LEN.

The server uses a UNIX socket named, for example:

#define UNIX_SK_NAME "Broadway"

The server takes one argument:

./sop-broadway <timeout>

timeout means the number of seconds without any new event after which the server should terminate, clean resources, remove the UNIX socket file, and exit.

Stage 1 — accepting connections, 5 pts

Create a UNIX stream socket.

Use epoll, poll, or select, but most likely the teacher expects epoll.

The server should:

create UNIX socket,
bind,
listen,
add the listening socket to epoll,
wait for incoming connections,
accept every new client,
print:
New spectator ([descriptor]) entered Broadway!
close the accepted client immediately.

If there is no connection for <timeout> seconds, print:

The show is over!

Then close all descriptors, remove the socket file, and terminate.

This stage is very similar to the previous group’s Stage 1, where the server accepted a UNIX socket connection, printed a message with the descriptor, closed it, and stopped after a timeout.

Stage 2 — keep clients connected, 7–8 pts

Now the server must not close clients immediately.

After accepting a client, add its descriptor to epoll.

The first complete line from every client is its role:

SOURCE

or

CLIENT <name>

Examples:

SOURCE
CLIENT Alice
CLIENT Bob

Rules:

There may be many clients.
There may be only one active source.
If nobody is the source yet, the first client that sends SOURCE\n becomes the source.
If another client sends SOURCE\n while a source already exists, the server sends back:
Source already exists

and closes that connection.
5. If a client disconnects before sending its first complete line, print:

Unknown client disconnected
If a normal client registers correctly, print:
Client [name] joined Broadway
If the source registers correctly, print:
Source entered Broadway

Important implementation detail: because stream sockets do not preserve message boundaries, you must buffer data until \n.

Stage 3 — broadcast source messages, 6–8 pts

When the source sends a complete line, the server broadcasts this line to every ordinary client.

Example:

Source sends:

Hello audience!

Server sends to every connected client:

Hello audience!

The server should also print:

Broadcasting: Hello audience!

Rules:

The source does not receive its own message.
Only messages from the source are broadcast.
Messages from ordinary clients are ignored, or optionally printed as an error:
Client [name] tried to speak
If a client disconnects, remove it from epoll, close its descriptor, and free its data.
If the source disconnects, print:
Source left Broadway

Then allow a new source to connect later.

This stage is the natural modification of the previous group’s Stage 4. In the previous task, messages were forwarded between matched clients; here, the same mechanism becomes one-to-many forwarding.

Stage 4 — server stdin also broadcasts, 5 pts

Add standard input STDIN_FILENO to epoll.

If the server operator types a complete line into terminal, the server broadcasts it to all connected ordinary clients.

Example:

Server stdin:

Server announcement

Every client receives:

Server announcement

Print:

Admin broadcast: Server announcement

If there are no connected clients, ignore the line or print:

No audience

This is very likely because the previous group’s final stage also added stdin to epoll, using lines typed into the server to forward messages to clients.
