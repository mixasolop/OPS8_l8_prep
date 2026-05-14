#include "l7-common.h"

#define UNIX_SK_NAME "Broadway"
#define BACKLOG_SIZE 16
#define MAX_EVENTS 16
#define MAX_CLIENTS 64
#define MAX_MSG_LEN 256
#define MAX_NAME_LEN 64

typedef enum
{
    UNKNOWN,
    SOURCE,
    CLIENT
} client_type_t;

typedef struct client
{
    int fd;
    client_type_t type;
    char name[MAX_NAME_LEN];
    char buff[MAX_MSG_LEN + 1];
    int buff_size;
} client_t;

void init_clients(client_t *clients)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        clients[i].fd = -1;
        clients[i].type = UNKNOWN;
        clients[i].name[0] = '\0';
        clients[i].buff[0] = '\0';
        clients[i].buff_size = 0;
    }
}

int find_free_client_index(client_t *clients)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd == -1)
            return i;
    }

    return -1;
}

int find_client_index(client_t *clients, int fd)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd == fd)
            return i;
    }

    return -1;
}

int source_exists(client_t *clients)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd != -1 && clients[i].type == SOURCE)
            return 1;
    }

    return 0;
}

void delete_client(client_t *clients, int epoll_descriptor, int fd)
{
    int client_idx = find_client_index(clients, fd);

    if (client_idx == -1)
        return;

    epoll_ctl(epoll_descriptor, EPOLL_CTL_DEL, fd, NULL);
    close(fd);

    clients[client_idx].fd = -1;
    clients[client_idx].type = UNKNOWN;
    clients[client_idx].name[0] = '\0';
    clients[client_idx].buff[0] = '\0';
    clients[client_idx].buff_size = 0;
}

void set_socket_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL);

    if (flags == -1)
        ERR("fcntl");

    flags |= O_NONBLOCK;

    if (fcntl(fd, F_SETFL, flags) == -1)
        ERR("fcntl");
}

void broadcast_to_clients(client_t *clients, int epoll_descriptor, char *line)
{
    char message[MAX_MSG_LEN + 2];

    snprintf(message, sizeof(message), "%s\n", line);

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd != -1 && clients[i].type == CLIENT)
        {
            if (bulk_write(clients[i].fd, message, strlen(message)) < 0)
            {
                delete_client(clients, epoll_descriptor, clients[i].fd);
            }
        }
    }
}

void process_client_line(client_t *clients, int epoll_descriptor, int client_idx, char *line)
{
    if (clients[client_idx].type == UNKNOWN)
    {
        if (strcmp(line, "SOURCE") == 0)
        {
            if (source_exists(clients))
            {
                char msg[] = "Source already exists\n";
                bulk_write(clients[client_idx].fd, msg, strlen(msg));
                delete_client(clients, epoll_descriptor, clients[client_idx].fd);
                return;
            }

            clients[client_idx].type = SOURCE;
            strcpy(clients[client_idx].name, "SOURCE");

            printf("Source entered Broadway\n");
            return;
        }

        if (strncmp(line, "CLIENT ", 7) == 0)
        {
            clients[client_idx].type = CLIENT;

            strncpy(clients[client_idx].name, line + 7, MAX_NAME_LEN - 1);
            clients[client_idx].name[MAX_NAME_LEN - 1] = '\0';

            printf("Client %s joined Broadway\n", clients[client_idx].name);
            return;
        }

        delete_client(clients, epoll_descriptor, clients[client_idx].fd);
        return;
    }

    if (clients[client_idx].type == SOURCE)
    {
        broadcast_to_clients(clients, epoll_descriptor, line);
        return;
    }

    if (clients[client_idx].type == CLIENT)
    {
        return;
    }
}

void process_client_buffer(client_t *clients, int epoll_descriptor, int client_idx)
{
    while (clients[client_idx].fd != -1)
    {
        char *newline_position = strchr(clients[client_idx].buff, '\n');

        if (newline_position == NULL)
            return;

        int line_len = newline_position - clients[client_idx].buff;

        char line[MAX_MSG_LEN + 1];
        memcpy(line, clients[client_idx].buff, line_len);
        line[line_len] = '\0';

        if (line_len > 0 && line[line_len - 1] == '\r')
            line[line_len - 1] = '\0';

        int used = line_len + 1;
        int remaining = clients[client_idx].buff_size - used;

        memmove(
            clients[client_idx].buff,
            clients[client_idx].buff + used,
            remaining
        );

        clients[client_idx].buff_size = remaining;
        clients[client_idx].buff[remaining] = '\0';

        process_client_line(clients, epoll_descriptor, client_idx, line);
    }
}

void doServer(int timeout)
{
    int local_listen_socket;
    int epoll_descriptor;
    client_t clients[MAX_CLIENTS];

    struct epoll_event event;
    struct epoll_event events[MAX_EVENTS];

    init_clients(clients);

    if (sethandler(SIG_IGN, SIGPIPE) == -1)
        ERR("sethandler");

    local_listen_socket = bind_local_socket(UNIX_SK_NAME, BACKLOG_SIZE);

    epoll_descriptor = epoll_create1(0);
    if (epoll_descriptor == -1)
        ERR("epoll_create1");

    memset(&event, 0, sizeof(struct epoll_event));

    event.events = EPOLLIN;
    event.data.fd = local_listen_socket;

    if (epoll_ctl(epoll_descriptor, EPOLL_CTL_ADD, local_listen_socket, &event) == -1)
        ERR("epoll_ctl");

    while (1)
    {
        int nfds = epoll_wait(epoll_descriptor, events, MAX_EVENTS, timeout * 1000);

        if (nfds == -1)
        {
            if (errno == EINTR)
                continue;

            ERR("epoll_wait");
        }

        if (nfds == 0)
        {
            printf("No one needs my help anymore!\n");
            break;
        }

        for (int i = 0; i < nfds; i++)
        {
            int fd = events[i].data.fd;

            if (fd == local_listen_socket)
            {
                int new_client = add_new_client(local_listen_socket);

                if (new_client == -1)
                    continue;

                int client_idx = find_free_client_index(clients);

                if (client_idx == -1)
                {
                    close(new_client);
                    continue;
                }

                set_socket_nonblock(new_client);

                clients[client_idx].fd = new_client;
                clients[client_idx].type = UNKNOWN;
                clients[client_idx].name[0] = '\0';
                clients[client_idx].buff[0] = '\0';
                clients[client_idx].buff_size = 0;

                memset(&event, 0, sizeof(struct epoll_event));

                event.events = EPOLLIN;
                event.data.fd = new_client;

                if (epoll_ctl(epoll_descriptor, EPOLL_CTL_ADD, new_client, &event) == -1)
                    ERR("epoll_ctl");
            }
            else
            {
                int client_idx = find_client_index(clients, fd);

                if (client_idx == -1)
                    continue;

                if (clients[client_idx].buff_size >= MAX_MSG_LEN)
                {
                    delete_client(clients, epoll_descriptor, fd);
                    continue;
                }

                ssize_t bytes_read = read(
                    fd,
                    clients[client_idx].buff + clients[client_idx].buff_size,
                    MAX_MSG_LEN - clients[client_idx].buff_size
                );

                if (bytes_read == -1)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;

                    ERR("read");
                }

                if (bytes_read == 0)
                {
                    if (clients[client_idx].type == SOURCE)
                        printf("Source left Broadway\n");

                    if (clients[client_idx].type == UNKNOWN)
                        printf("Unknown client disconnected\n");

                    delete_client(clients, epoll_descriptor, fd);
                    continue;
                }

                clients[client_idx].buff_size += bytes_read;
                clients[client_idx].buff[clients[client_idx].buff_size] = '\0';

                process_client_buffer(clients, epoll_descriptor, client_idx);
            }
        }
    }

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd != -1)
            close(clients[i].fd);
    }

    close(epoll_descriptor);
    close(local_listen_socket);
    unlink(UNIX_SK_NAME);
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        fprintf(stderr, "Usage: %s timeout\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int timeout = atoi(argv[1]);

    if (timeout <= 0)
    {
        fprintf(stderr, "Timeout must be positive.\n");
        exit(EXIT_FAILURE);
    }

    doServer(timeout);

    return EXIT_SUCCESS;
}
