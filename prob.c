#include "l7-common.h"

void usage(char *name)
{
    printf("%s <connect_port> <bind_port>\n", name);
    printf("  connect_port - port to receive messages from\n");
    printf("  bind_port - port to accept subscribers\n");
    exit(EXIT_FAILURE);
}

#define SWAP(a, b)                      \
    ({                                  \
        char c[sizeof(*(a))];           \
        memcpy(c, (a), sizeof(*(a)));   \
        memcpy((a), (b), sizeof(*(a))); \
        memcpy((b), (c), sizeof(*(a))); \
    })

#define MAX_CLIENTS 10
#define READ_BUFF_SIZE 64
#define GREP_LEN 5
#define BACKLOG 10

typedef struct client
{
    int fd;
} client_t;

void init_clients(client_t *clients)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        clients[i].fd = -1;
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

void set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL);

    if (flags == -1)
        ERR("fcntl");

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
        ERR("fcntl");
}

void delete_client(client_t *clients, int epoll_fd, int fd)
{
    int client_idx = find_client_index(clients, fd);

    if (client_idx == -1)
        return;

    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);

    clients[client_idx].fd = -1;
}

void delete_all_clients(client_t *clients)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd != -1)
        {
            close(clients[i].fd);
            clients[i].fd = -1;
        }
    }
}

void doServer(char *connect_port, uint16_t bind_port)
{
    int source_fd;
    int listen_fd;
    int epoll_fd;

    client_t clients[MAX_CLIENTS];

    struct epoll_event event;
    struct epoll_event events[MAX_CLIENTS + 2];

    init_clients(clients);

    if (sethandler(SIG_IGN, SIGPIPE) == -1)
        ERR("sethandler");

    source_fd = connect_tcp_socket("localhost", connect_port);
    listen_fd = bind_tcp_socket(bind_port, BACKLOG);

    set_nonblock(source_fd);
    set_nonblock(listen_fd);

    epoll_fd = epoll_create1(0);

    if (epoll_fd == -1)
        ERR("epoll_create1");

    memset(&event, 0, sizeof(event));

    event.events = EPOLLIN;
    event.data.fd = source_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, source_fd, &event) == -1)
        ERR("epoll_ctl source_fd");

    event.events = EPOLLIN;
    event.data.fd = listen_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &event) == -1)
        ERR("epoll_ctl listen_fd");

    while (1)
    {
        int nfds = epoll_wait(epoll_fd, events, MAX_CLIENTS + 2, -1);

        if (nfds == -1)
        {
            if (errno == EINTR)
                continue;

            ERR("epoll_wait");
        }

        for (int i = 0; i < nfds; i++)
        {
            int fd = events[i].data.fd;

            if (fd == listen_fd)
            {
                int new_client = add_new_client(listen_fd);

                if (new_client == -1)
                    continue;

                int client_idx = find_free_client_index(clients);

                if (client_idx == -1)
                {
                    close(new_client);
                    continue;
                }

                set_nonblock(new_client);

                clients[client_idx].fd = new_client;

                event.events = EPOLLIN;
                event.data.fd = new_client;

                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, new_client, &event) == -1)
                    ERR("epoll_ctl new_client");

                printf("New subscriber %d\n", new_client);
            }
            else if (fd == source_fd)
            {
                char buffer[READ_BUFF_SIZE];

                ssize_t bytes_read = read(source_fd, buffer, READ_BUFF_SIZE);

                if (bytes_read == -1)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;

                    ERR("read source_fd");
                }

                if (bytes_read == 0)
                {
                    printf("Source disconnected\n");

                    delete_all_clients(clients);
                    close(source_fd);
                    close(listen_fd);
                    close(epoll_fd);

                    return;
                }

                /*
                    Part 1:
                    We only read from source so epoll will not loop forever.
                    Forwarding messages will be done in the next part.
                */
            }
            else
            {
                char buffer[READ_BUFF_SIZE];

                ssize_t bytes_read = read(fd, buffer, READ_BUFF_SIZE);

                if (bytes_read == -1)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;

                    ERR("read client");
                }

                if (bytes_read == 0)
                {
                    delete_client(clients, epoll_fd, fd);
                    continue;
                }

                /*
                    Part 1:
                    We only read from subscribers so epoll will not loop forever.
                    Subscriber filters will be handled in the next part.
                */
            }
        }
    }
}

int main(int argc, char **argv)
{
    if (argc != 3)
    {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    char *connect_port = argv[1];
    uint16_t bind_port = atoi(argv[2]);

    doServer(connect_port, bind_port);

    return EXIT_SUCCESS;
}




// PART 2



#include "l7-common.h"

void usage(char *name)
{
    printf("%s <connect_port> <bind_port>\n", name);
    printf("  connect_port - port to receive messages from\n");
    printf("  bind_port - port to accept subscribers\n");
    exit(EXIT_FAILURE);
}

#define SWAP(a, b)                      \
    ({                                  \
        char c[sizeof(*(a))];           \
        memcpy(c, (a), sizeof(*(a)));   \
        memcpy((a), (b), sizeof(*(a))); \
        memcpy((b), (c), sizeof(*(a))); \
    })

#define MAX_CLIENTS 10
#define READ_BUFF_SIZE 64
#define GREP_LEN 5
#define BACKLOG 10

typedef struct client
{
    int fd;
    char grep[GREP_LEN + 1];
    int grep_size;
} client_t;

void init_clients(client_t *clients)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        clients[i].fd = -1;
        clients[i].grep[0] = '\0';
        clients[i].grep_size = 0;
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

void set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL);

    if (flags == -1)
        ERR("fcntl");

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
        ERR("fcntl");
}

void delete_client(client_t *clients, int epoll_fd, int fd)
{
    int client_idx = find_client_index(clients, fd);

    if (client_idx == -1)
        return;

    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);

    clients[client_idx].fd = -1;
    clients[client_idx].grep[0] = '\0';
    clients[client_idx].grep_size = 0;
}

void delete_all_clients(client_t *clients)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd != -1)
        {
            close(clients[i].fd);
            clients[i].fd = -1;
            clients[i].grep[0] = '\0';
            clients[i].grep_size = 0;
        }
    }
}

void read_client_grep(client_t *clients, int epoll_fd, int fd)
{
    int client_idx = find_client_index(clients, fd);

    if (client_idx == -1)
        return;

    if (clients[client_idx].grep_size >= GREP_LEN)
    {
        char trash[READ_BUFF_SIZE];

        ssize_t bytes_read = read(fd, trash, READ_BUFF_SIZE);

        if (bytes_read == -1)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;

            ERR("read");
        }

        if (bytes_read == 0)
            delete_client(clients, epoll_fd, fd);

        return;
    }

    ssize_t bytes_read = read(
        fd,
        clients[client_idx].grep + clients[client_idx].grep_size,
        GREP_LEN - clients[client_idx].grep_size
    );

    if (bytes_read == -1)
    {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;

        ERR("read");
    }

    if (bytes_read == 0)
    {
        delete_client(clients, epoll_fd, fd);
        return;
    }

    clients[client_idx].grep_size += bytes_read;

    if (clients[client_idx].grep_size == GREP_LEN)
    {
        clients[client_idx].grep[GREP_LEN] = '\0';
        printf("Subscriber %d filters %s\n", fd, clients[client_idx].grep);
    }
}

void doServer(char *connect_port, uint16_t bind_port)
{
    int source_fd;
    int listen_fd;
    int epoll_fd;

    client_t clients[MAX_CLIENTS];

    struct epoll_event event;
    struct epoll_event events[MAX_CLIENTS + 2];

    init_clients(clients);

    if (sethandler(SIG_IGN, SIGPIPE) == -1)
        ERR("sethandler");

    source_fd = connect_tcp_socket("localhost", connect_port);
    listen_fd = bind_tcp_socket(bind_port, BACKLOG);

    set_nonblock(source_fd);
    set_nonblock(listen_fd);

    epoll_fd = epoll_create1(0);

    if (epoll_fd == -1)
        ERR("epoll_create1");

    memset(&event, 0, sizeof(event));

    event.events = EPOLLIN;
    event.data.fd = source_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, source_fd, &event) == -1)
        ERR("epoll_ctl source_fd");

    event.events = EPOLLIN;
    event.data.fd = listen_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &event) == -1)
        ERR("epoll_ctl listen_fd");

    while (1)
    {
        int nfds = epoll_wait(epoll_fd, events, MAX_CLIENTS + 2, -1);

        if (nfds == -1)
        {
            if (errno == EINTR)
                continue;

            ERR("epoll_wait");
        }

        for (int i = 0; i < nfds; i++)
        {
            int fd = events[i].data.fd;

            if (fd == listen_fd)
            {
                int new_client = add_new_client(listen_fd);

                if (new_client == -1)
                    continue;

                int client_idx = find_free_client_index(clients);

                if (client_idx == -1)
                {
                    close(new_client);
                    continue;
                }

                set_nonblock(new_client);

                clients[client_idx].fd = new_client;
                clients[client_idx].grep[0] = '\0';
                clients[client_idx].grep_size = 0;

                event.events = EPOLLIN;
                event.data.fd = new_client;

                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, new_client, &event) == -1)
                    ERR("epoll_ctl new_client");

                printf("New subscriber %d\n", new_client);
            }
            else if (fd == source_fd)
            {
                char buffer[READ_BUFF_SIZE];

                ssize_t bytes_read = read(source_fd, buffer, READ_BUFF_SIZE);

                if (bytes_read == -1)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;

                    ERR("read source_fd");
                }

                if (bytes_read == 0)
                {
                    printf("Source disconnected\n");

                    delete_all_clients(clients);
                    close(source_fd);
                    close(listen_fd);
                    close(epoll_fd);

                    return;
                }

                /*
                    Part 2:
                    We only drain source messages.
                    Forwarding will be done in Part 3.
                */
            }
            else
            {
                read_client_grep(clients, epoll_fd, fd);
            }
        }
    }
}

int main(int argc, char **argv)
{
    if (argc != 3)
    {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    char *connect_port = argv[1];
    uint16_t bind_port = atoi(argv[2]);

    doServer(connect_port, bind_port);

    return EXIT_SUCCESS;
}




// PART 3



#include "l7-common.h"

void usage(char *name)
{
    printf("%s <connect_port> <bind_port>\n", name);
    printf("  connect_port - port to receive messages from\n");
    printf("  bind_port - port to accept subscribers\n");
    exit(EXIT_FAILURE);
}

#define SWAP(a, b)                      \
    ({                                  \
        char c[sizeof(*(a))];           \
        memcpy(c, (a), sizeof(*(a)));   \
        memcpy((a), (b), sizeof(*(a))); \
        memcpy((b), (c), sizeof(*(a))); \
    })

#define MAX_CLIENTS 10
#define READ_BUFF_SIZE 64
#define GREP_LEN 5
#define BACKLOG 10

typedef struct client
{
    int fd;
    char grep[GREP_LEN + 1];
    int grep_size;
} client_t;

void init_clients(client_t *clients)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        clients[i].fd = -1;
        clients[i].grep[0] = '\0';
        clients[i].grep_size = 0;
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

void set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL);

    if (flags == -1)
        ERR("fcntl");

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
        ERR("fcntl");
}

void delete_client(client_t *clients, int epoll_fd, int fd)
{
    int client_idx = find_client_index(clients, fd);

    if (client_idx == -1)
        return;

    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);

    clients[client_idx].fd = -1;
    clients[client_idx].grep[0] = '\0';
    clients[client_idx].grep_size = 0;
}

void delete_all_clients(client_t *clients)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd != -1)
        {
            close(clients[i].fd);
            clients[i].fd = -1;
            clients[i].grep[0] = '\0';
            clients[i].grep_size = 0;
        }
    }
}

void read_client_grep(client_t *clients, int epoll_fd, int fd)
{
    int client_idx = find_client_index(clients, fd);

    if (client_idx == -1)
        return;

    if (clients[client_idx].grep_size >= GREP_LEN)
    {
        char trash[READ_BUFF_SIZE];

        ssize_t bytes_read = read(fd, trash, READ_BUFF_SIZE);

        if (bytes_read == -1)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;

            ERR("read");
        }

        if (bytes_read == 0)
            delete_client(clients, epoll_fd, fd);

        return;
    }

    ssize_t bytes_read = read(
        fd,
        clients[client_idx].grep + clients[client_idx].grep_size,
        GREP_LEN - clients[client_idx].grep_size
    );

    if (bytes_read == -1)
    {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;

        ERR("read");
    }

    if (bytes_read == 0)
    {
        delete_client(clients, epoll_fd, fd);
        return;
    }

    clients[client_idx].grep_size += bytes_read;

    if (clients[client_idx].grep_size == GREP_LEN)
    {
        clients[client_idx].grep[GREP_LEN] = '\0';
        printf("Subscriber %d filters %s\n", fd, clients[client_idx].grep);
    }
}

void send_to_matching_clients(client_t *clients, int epoll_fd, char *message, int message_len)
{
    char safe_message[READ_BUFF_SIZE + 1];

    memcpy(safe_message, message, message_len);
    safe_message[message_len] = '\0';

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd == -1)
            continue;

        if (clients[i].grep_size < GREP_LEN)
            continue;

        if (strstr(safe_message, clients[i].grep) != NULL)
        {
            if (bulk_write(clients[i].fd, message, message_len) < 0)
            {
                if (errno == EPIPE)
                {
                    delete_client(clients, epoll_fd, clients[i].fd);
                    continue;
                }

                ERR("bulk_write");
            }
        }
    }
}

void doServer(char *connect_port, uint16_t bind_port)
{
    int source_fd;
    int listen_fd;
    int epoll_fd;

    client_t clients[MAX_CLIENTS];

    struct epoll_event event;
    struct epoll_event events[MAX_CLIENTS + 2];

    init_clients(clients);

    if (sethandler(SIG_IGN, SIGPIPE) == -1)
        ERR("sethandler");

    source_fd = connect_tcp_socket("localhost", connect_port);
    listen_fd = bind_tcp_socket(bind_port, BACKLOG);

    set_nonblock(source_fd);
    set_nonblock(listen_fd);

    epoll_fd = epoll_create1(0);

    if (epoll_fd == -1)
        ERR("epoll_create1");

    memset(&event, 0, sizeof(event));

    event.events = EPOLLIN;
    event.data.fd = source_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, source_fd, &event) == -1)
        ERR("epoll_ctl source_fd");

    event.events = EPOLLIN;
    event.data.fd = listen_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &event) == -1)
        ERR("epoll_ctl listen_fd");

    while (1)
    {
        int nfds = epoll_wait(epoll_fd, events, MAX_CLIENTS + 2, -1);

        if (nfds == -1)
        {
            if (errno == EINTR)
                continue;

            ERR("epoll_wait");
        }

        for (int i = 0; i < nfds; i++)
        {
            int fd = events[i].data.fd;

            if (fd == listen_fd)
            {
                int new_client = add_new_client(listen_fd);

                if (new_client == -1)
                    continue;

                int client_idx = find_free_client_index(clients);

                if (client_idx == -1)
                {
                    close(new_client);
                    continue;
                }

                set_nonblock(new_client);

                clients[client_idx].fd = new_client;
                clients[client_idx].grep[0] = '\0';
                clients[client_idx].grep_size = 0;

                event.events = EPOLLIN;
                event.data.fd = new_client;

                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, new_client, &event) == -1)
                    ERR("epoll_ctl new_client");

                printf("New subscriber %d\n", new_client);
            }
            else if (fd == source_fd)
            {
                char buffer[READ_BUFF_SIZE + 1];

                ssize_t bytes_read = read(source_fd, buffer, READ_BUFF_SIZE);

                if (bytes_read == -1)
                {
                    if (errno == EAGAIN || errno == EWOULDBLOCK)
                        continue;

                    ERR("read source_fd");
                }

                if (bytes_read == 0)
                {
                    printf("Source disconnected\n");

                    delete_all_clients(clients);
                    close(source_fd);
                    close(listen_fd);
                    close(epoll_fd);

                    return;
                }

                buffer[bytes_read] = '\0';

                printf("Received: %s", buffer);

                send_to_matching_clients(clients, epoll_fd, buffer, bytes_read);
            }
            else
            {
                read_client_grep(clients, epoll_fd, fd);
            }
        }
    }
}

int main(int argc, char **argv)
{
    if (argc != 3)
    {
        usage(argv[0]);
        exit(EXIT_FAILURE);
    }

    char *connect_port = argv[1];
    uint16_t bind_port = atoi(argv[2]);

    doServer(connect_port, bind_port);

    return EXIT_SUCCESS;
}
