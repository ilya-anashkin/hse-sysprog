#include "chat_client.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <unistd.h>

#include "chat.h"

static int parse_host_port(const char *addr, char *host, size_t host_size,
                           uint16_t *port) {
  const char *colon = strrchr(addr, ':');
  if (!colon || colon == addr || *(colon + 1) == '\0') return -1;

  size_t host_len = colon - addr;
  if (host_len >= host_size) return -1;

  strncpy(host, addr, host_len);
  host[host_len] = '\0';

  char *endptr;
  long port_num = strtol(colon + 1, &endptr, 10);
  if (*endptr != '\0' || port_num <= 0 || port_num > 65535) return -1;

  *port = (uint16_t)port_num;
  return 0;
}

struct chat_client {
  /** Socket connected to the server. */
  int socket;

  /** Array of received messages. */
  struct chat_message *received_messages;
  size_t received_count;
  size_t received_capacity;

  /** Output buffer. */
  char *output_buffer;
  size_t output_size;
  size_t output_capacity;

  /** Input buffer. **/
  char *input_buffer;
  size_t input_size;
  size_t input_capacity;

  int kqueue_fd;
};

struct chat_client *chat_client_new(const char *name) {
  /* Ignore 'name' param if don't want to support it for +5 points. */
  (void)name;

  struct chat_client *client = calloc(1, sizeof(*client));
  client->socket = -1;

  client->received_messages = NULL;
  client->received_count = 0;
  client->received_capacity = 0;

  client->output_buffer = NULL;
  client->output_size = 0;
  client->output_capacity = 0;

  client->input_buffer = NULL;
  client->input_size = 0;
  client->input_capacity = 0;

  return client;
}

void chat_client_delete(struct chat_client *client) {
  if (client->socket >= 0) close(client->socket);
  if (client->kqueue_fd >= 0) close(client->kqueue_fd);

  // for (size_t i = 0; i < client->received_count; i++) {
  //   free(client->received_messages[i].data);
  // }
  // free(client->received_messages);

  free(client->output_buffer);
  free(client->input_buffer);

  free(client);
}

int chat_client_connect(struct chat_client *client, const char *addr) {
  /*
   * 1) Use getaddrinfo() to resolve addr to struct sockaddr_in.
   * 2) Create a client socket (function socket()).
   * 3) Connect it by the found address (function connect()).
   */

  char host[256];
  uint16_t port;
  if (parse_host_port(addr, host, sizeof(host), &port) != 0)
    return CHAT_ERR_SYS;

  struct addrinfo hints = {0}, *res = NULL;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  char port_str[6];
  snprintf(port_str, sizeof(port_str), "%u", port);

  if (getaddrinfo(host, port_str, &hints, &res) != 0) return CHAT_ERR_SYS;

  client->socket = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (client->socket == -1) {
    freeaddrinfo(res);
    return CHAT_ERR_SYS;
  }

  // Set socket to non-blocking mode
  int flags = fcntl(client->socket, F_GETFL, 0);
  if (flags == -1 || fcntl(client->socket, F_SETFL, flags | O_NONBLOCK) == -1) {
    close(client->socket);
    freeaddrinfo(res);
    return CHAT_ERR_SYS;
  }

  if (connect(client->socket, res->ai_addr, res->ai_addrlen) == -1 &&
      errno != EINPROGRESS) {
    close(client->socket);
    freeaddrinfo(res);
    return CHAT_ERR_SYS;
  }

  freeaddrinfo(res);

  // Create kqueue
  client->kqueue_fd = kqueue();
  if (client->kqueue_fd == -1) {
    close(client->socket);
    return CHAT_ERR_SYS;
  }

  // Register the socket for events
  struct kevent ev;
  EV_SET(&ev, client->socket, EVFILT_READ, EV_ADD, 0, 0, NULL);
  if (kevent(client->kqueue_fd, &ev, 1, NULL, 0, NULL) == -1) {
    close(client->socket);
    close(client->kqueue_fd);
    return CHAT_ERR_SYS;
  }

  return 0;
}

struct chat_message *chat_client_pop_next(struct chat_client *client) {
  if (client->received_count == 0) return NULL;

  struct chat_message *original_msg = &client->received_messages[0];
  printf("[CLIENT] pop msg: %p, data: %p\n", original_msg, original_msg->data);

  // Создаем копию сообщения
  struct chat_message *msg = malloc(sizeof(*msg));
  if (!msg) return NULL;

  msg->data = strdup(original_msg->data);  // Копируем данные
  if (!msg->data) {
    free(msg);
    return NULL;
  }

  // Сдвигаем очередь сообщений
  memmove(client->received_messages, client->received_messages + 1,
          (client->received_count - 1) * sizeof(*client->received_messages));
  client->received_count--;

  return msg;
}

int chat_client_update(struct chat_client *client, double timeout) {
  /*
   * The easiest way to wait for updates on a single socket with a timeout
   * is to use poll(). Epoll is good for many sockets, poll is good for a
   * few.
   *
   * You create one struct pollfd, fill it, call poll() on it, handle the
   * events (do read/write).
   */

  if (client->socket == -1) return CHAT_ERR_NOT_STARTED;

  if (timeout == 0) {
    timeout = 0.001;  // magic fix
  }

  struct kevent events[2];
  struct timespec ts = {.tv_sec = (int)timeout,
                        .tv_nsec = (timeout - (int)timeout) * 1e9};

  int nev = kevent(client->kqueue_fd, NULL, 0, events, 2, &ts);
  // printf("[CLIENT] kevent end, nev = %d\n", nev);
  if (nev == -1) return CHAT_ERR_SYS;
  if (nev == 0) return CHAT_ERR_TIMEOUT;  // Таймаут без событий

  for (int i = 0; i < nev; i++) {
    if (events[i].filter == EVFILT_READ) {
      // Чтение данных из сокета
      char buffer[1024];
      ssize_t n = read(client->socket, buffer, sizeof(buffer));
      if (n > 0) {
        printf("[CLIENT] read n = %zd\n", n);
        // Обработка полученных данных
        for (ssize_t j = 0; j < n; j++) {
          if (buffer[j] == '\n') {
            // Завершено сообщение
            if (client->input_size > 0) {
              // Добавляем сообщение в массив
              struct chat_message *msg = malloc(sizeof(*msg));
              msg->data = malloc(client->input_size + 1);
              memcpy(msg->data, client->input_buffer, client->input_size);
              msg->data[client->input_size] = '\0';

              if (client->received_count == client->received_capacity) {
                size_t new_capacity = client->received_capacity == 0
                                          ? 4
                                          : client->received_capacity * 2;
                struct chat_message *new_array =
                    realloc(client->received_messages,
                            new_capacity * sizeof(*client->received_messages));
                if (!new_array) {
                  // free(msg->data);
                  // free(msg);
                  return CHAT_ERR_SYS;
                }
                client->received_messages = new_array;
                client->received_capacity = new_capacity;
              }

              client->received_messages[client->received_count++] = *msg;
              // free(msg);
              client->input_size = 0;
              // printf("[CLIENT] added msg to queue\n");
            }
          } else {
            // Добавляем символ в буфер ввода
            if (client->input_size == client->input_capacity) {
              size_t new_capacity = client->input_capacity == 0
                                        ? 128
                                        : client->input_capacity * 2;
              char *new_buffer = realloc(client->input_buffer, new_capacity);
              if (!new_buffer) return CHAT_ERR_SYS;
              client->input_buffer = new_buffer;
              client->input_capacity = new_capacity;
            }
            client->input_buffer[client->input_size++] = buffer[j];
            // printf("[CLIENT] added symbol to buffer[%zd] = %c\n",
            //        client->input_size - 1, buffer[j]);
          }
        }
      } else if (n == 0) {
        return CHAT_ERR_TIMEOUT;  // Сервер закрыл соединение
      }
    } else if (events[i].filter == EVFILT_WRITE) {
      // Отправка данных из буфера вывода
      ssize_t n =
          write(client->socket, client->output_buffer, client->output_size);
      if (n > 0) {
        printf("[CLIENT] write n = %zd\n", n);
        memmove(client->output_buffer, client->output_buffer + n,
                client->output_size - n);
        client->output_size -= n;

        if (client->output_size == 0) {
          struct kevent ev;
          EV_SET(&ev, client->socket, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
          kevent(client->kqueue_fd, &ev, 1, NULL, 0, NULL);
        }
      }
    }
  }

  return 0;
}

int chat_client_get_descriptor(const struct chat_client *client) {
  return client->socket;
}

int chat_client_get_events(const struct chat_client *client) {
  if (client->socket == -1) return 0;

  int events = CHAT_EVENT_INPUT;  // Всегда ожидаем входящих данных

  // Если буфер вывода не пуст, добавляем событие OUTPUT
  if (client->output_size > 0) {
    events |= CHAT_EVENT_OUTPUT;
  }

  return events;
}

int chat_client_feed(struct chat_client *client, const char *msg,
                     uint32_t msg_size) {
  if (client->socket == -1) return CHAT_ERR_NOT_STARTED;

  if (msg_size == 0) return 0;

  if (client->output_size + msg_size > client->output_capacity) {
    size_t new_capacity = (client->output_size + msg_size) * 2;
    char *new_buffer = realloc(client->output_buffer, new_capacity);
    if (!new_buffer) return CHAT_ERR_SYS;

    client->output_buffer = new_buffer;
    client->output_capacity = new_capacity;
  }

  memcpy(client->output_buffer + client->output_size, msg, msg_size);
  client->output_size += msg_size;

  // Регистрируем событие EVFILT_WRITE в kqueue
  struct kevent ev;
  EV_SET(&ev, client->socket, EVFILT_WRITE, EV_ADD, 0, 0, NULL);
  if (kevent(client->kqueue_fd, &ev, 1, NULL, 0, NULL) == -1) {
    return CHAT_ERR_SYS;
  }

  return 0;
}
