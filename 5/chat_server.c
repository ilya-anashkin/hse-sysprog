#include "chat_server.h"

#include <netinet/in.h>
#include <stdio.h>  // For debugging
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <unistd.h>

#include "chat.h"

struct chat_peer {
  /** Client's socket. To read/write messages. */
  int socket;

  /** Output buffer. */
  char *output_buffer;
  size_t output_size;
  size_t output_capacity;

  /** Input buffer. */
  char *input_buffer;
  size_t input_size;
  size_t input_capacity;
};

struct chat_server {
  /** Listening socket. To accept new clients. */
  int socket;

  /** Array of peers. */
  struct chat_peer *peers;
  size_t peer_count;
  size_t peer_capacity;

  /** Queue of messages. */
  struct chat_message *messages;
  size_t message_count;
  size_t message_capacity;

  int kqueue_fd;
};

struct chat_server *chat_server_new(void) {
  struct chat_server *server = calloc(1, sizeof(*server));
  server->socket = -1;

  server->kqueue_fd = kqueue();
  if (server->kqueue_fd == -1) {
    free(server);
    return NULL;
  }

  return server;
}

void chat_server_delete(struct chat_server *server) {
  if (server->socket >= 0) close(server->socket);

  if (server->kqueue_fd >= 0) close(server->kqueue_fd);

  for (size_t i = 0; i < server->peer_count; i++) {
    close(server->peers[i].socket);
    free(server->peers[i].output_buffer);
    free(server->peers[i].input_buffer);
  }
  free(server->peers);

  // for (size_t i = 0; i < server->message_count; i++) {
  //   free(server->messages[i].data);
  // }
  // free(server->messages);

  free(server);
}

int chat_server_listen(struct chat_server *server, uint16_t port) {
  //   struct sockaddr_in addr;
  //   memset(&addr, 0, sizeof(addr));
  //   addr.sin_port = htons(port);
  //   /* Listen on all IPs of this machine. */
  //   addr.sin_addr.s_addr = htonl(INADDR_ANY);

  /*
   * 1) Create a server socket (function socket()).
   * 2) Bind the server socket to addr (function bind()).
   * 3) Listen the server socket (function listen()).
   * 4) Create epoll/kqueue if needed.
   */

  if (server->socket >= 0) return CHAT_ERR_ALREADY_STARTED;

  server->socket = socket(AF_INET, SOCK_STREAM, 0);
  if (server->socket == -1) return CHAT_ERR_SYS;

  int opt = 1;
  setsockopt(server->socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in addr = {0};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(server->socket, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    close(server->socket);
    server->socket = -1;
    return CHAT_ERR_PORT_BUSY;
  }

  if (listen(server->socket, 128) == -1) {
    close(server->socket);
    server->socket = -1;
    return CHAT_ERR_SYS;
  }

  // Добавляем сокет в kqueue
  struct kevent ev;
  EV_SET(&ev, server->socket, EVFILT_READ, EV_ADD, 0, 0, NULL);
  if (kevent(server->kqueue_fd, &ev, 1, NULL, 0, NULL) == -1) {
    close(server->socket);
    server->socket = -1;
    return CHAT_ERR_SYS;
  }

  return 0;
}

struct chat_message *chat_server_pop_next(struct chat_server *server) {
  printf("[SERVER] pop next message. Size: %zu\n", server->message_count);
  if (server->message_count == 0) return NULL;

  struct chat_message *original_msg = &server->messages[0];
  // printf("[SERVER] Returning message from queue: %p, data: %p\n",
  // original_msg,
  //        original_msg->data);

  // Создаем копию сообщения
  struct chat_message *msg = malloc(sizeof(*msg));
  if (!msg) return NULL;

  msg->data = strdup(original_msg->data);  // Копируем данные
  if (!msg->data) {
    free(msg);
    return NULL;
  }

  // Сдвигаем очередь сообщений
  memmove(server->messages, server->messages + 1,
          (server->message_count - 1) * sizeof(*server->messages));
  server->message_count--;

  return msg;
}

int chat_server_update(struct chat_server *server, double timeout) {
  /*
   * 1) Wait on epoll/kqueue/poll for update on any socket.
   * 2) Handle the update.
   * 2.1) If the update was on listen-socket, then you probably need to
   *     call accept() on it - a new client wants to join.
   * 2.2) If the update was on a client-socket, then you might want to
   *     read/write on it.
   */
  if (server->socket == -1) return CHAT_ERR_NOT_STARTED;

  if (timeout == 0) {
    timeout = 0.001;  // magic fix
  }

  struct kevent events[128];
  struct timespec ts = {.tv_sec = (int)timeout,
                        .tv_nsec = (timeout - (int)timeout) * 1e9};

  int nev = kevent(server->kqueue_fd, NULL, 0, events, 128, &ts);
  // printf("[SERVER] kevent end, nev = %d\n", nev);
  if (nev == -1) return CHAT_ERR_SYS;
  if (nev == 0) return CHAT_ERR_TIMEOUT;  // Таймаут без событий

  for (int i = 0; i < nev; i++) {
    if (events[i].ident == (uintptr_t)server->socket) {
      // Новое подключение
      int client_socket = accept(server->socket, NULL, NULL);
      printf("[SERVER] accept client_socket = %d\n", client_socket);
      if (client_socket >= 0) {
        // Добавляем клиента в массив
        if (server->peer_count == server->peer_capacity) {
          size_t new_capacity =
              server->peer_capacity == 0 ? 4 : server->peer_capacity * 2;
          struct chat_peer *new_peers =
              realloc(server->peers, new_capacity * sizeof(*server->peers));
          if (!new_peers) {
            close(client_socket);
            continue;
          }
          server->peers = new_peers;
          server->peer_capacity = new_capacity;
        }

        struct chat_peer *peer = &server->peers[server->peer_count++];
        peer->socket = client_socket;
        peer->output_buffer = NULL;
        peer->output_size = 0;
        peer->output_capacity = 0;
        peer->input_buffer = NULL;
        peer->input_size = 0;
        peer->input_capacity = 0;

        // Добавляем клиента в kqueue
        struct kevent ev;
        EV_SET(&ev, client_socket, EVFILT_READ, EV_ADD, 0, 0, peer);
        kevent(server->kqueue_fd, &ev, 1, NULL, 0, NULL);
      }
    } else {
      // Обработка данных от клиента
      struct chat_peer *peer = (struct chat_peer *)events[i].udata;
      // printf("[SERVER] process client_socket = %d\n", peer->socket);
      if (events[i].filter == EVFILT_READ) {
        char buffer[1024];
        ssize_t n = read(peer->socket, buffer, sizeof(buffer));
        if (n > 0) {
          printf("[SERVER] read n = %zd\n", n);
          // Обработка входящих данных
          for (ssize_t j = 0; j < n; j++) {
            if (buffer[j] == '\n') {
              // Завершено сообщение
              if (peer->input_size > 0) {
                // Добавляем сообщение в очередь
                if (server->message_count == server->message_capacity) {
                  size_t new_capacity = server->message_capacity == 0
                                            ? 4
                                            : server->message_capacity * 2;
                  struct chat_message *new_messages =
                      realloc(server->messages,
                              new_capacity * sizeof(*server->messages));
                  if (!new_messages) return CHAT_ERR_SYS;
                  server->messages = new_messages;
                  server->message_capacity = new_capacity;
                }

                struct chat_message *msg =
                    &server->messages[server->message_count++];
                msg->data = malloc(peer->input_size + 1);
                memcpy(msg->data, peer->input_buffer, peer->input_size);
                msg->data[peer->input_size] = '\0';

                peer->input_size = 0;
                // printf("[SERVER] added msg to queue\n");

                for (size_t k = 0; k < server->peer_count; k++) {
                  struct chat_peer *other_peer = &server->peers[k];
                  if (other_peer == peer)
                    continue;  // Не отправляем сообщение отправителю

                  size_t new_output_size =
                      other_peer->output_size + strlen(msg->data) + 1;
                  if (new_output_size > other_peer->output_capacity) {
                    size_t new_capacity = new_output_size * 2;
                    char *new_buffer =
                        realloc(other_peer->output_buffer, new_capacity);
                    if (!new_buffer) return CHAT_ERR_SYS;
                    other_peer->output_buffer = new_buffer;
                    other_peer->output_capacity = new_capacity;
                  }

                  memcpy(other_peer->output_buffer + other_peer->output_size,
                         msg->data, strlen(msg->data));
                  other_peer->output_size += strlen(msg->data);
                  other_peer->output_buffer[other_peer->output_size++] = '\n';

                  // Регистрируем событие EVFILT_WRITE для клиента
                  struct kevent ev;
                  EV_SET(&ev, other_peer->socket, EVFILT_WRITE, EV_ADD, 0, 0,
                         other_peer);
                  kevent(server->kqueue_fd, &ev, 1, NULL, 0, NULL);
                }
              }
            } else {
              // Добавляем символ в буфер ввода
              if (peer->input_size == peer->input_capacity) {
                size_t new_capacity =
                    peer->input_capacity == 0 ? 128 : peer->input_capacity * 2;
                char *new_buffer = realloc(peer->input_buffer, new_capacity);
                if (!new_buffer) return CHAT_ERR_SYS;
                peer->input_buffer = new_buffer;
                peer->input_capacity = new_capacity;
              }
              peer->input_buffer[peer->input_size++] = buffer[j];
              // printf("[SERVER] added symbol to buffer[%zd] = %c\n",
              //        peer->input_size - 1, buffer[j]);
            }
          }
        } else {
          // Клиент отключился
          close(peer->socket);
          *peer = server->peers[--server->peer_count];
        }
      } else if (events[i].filter == EVFILT_WRITE) {
        // Отправка данных клиенту
        ssize_t n = write(peer->socket, peer->output_buffer, peer->output_size);
        if (n > 0) {
          memmove(peer->output_buffer, peer->output_buffer + n,
                  peer->output_size - n);
          peer->output_size -= n;
        }
      }
    }
  }

  return nev == 0 ? CHAT_ERR_TIMEOUT : 0;
}

int chat_server_get_descriptor(const struct chat_server *server) {
#if NEED_SERVER_FEED
  /* IMPLEMENT THIS FUNCTION if want +5 points. */

  /*
   * Server has multiple sockets - own and from connected clients. Hence
   * you can't return a socket here. But if you are using epoll/kqueue,
   * then you can return their descriptor. These descriptors can be polled
   * just like sockets and will return an event when any of their owned
   * descriptors has any events.
   *
   * For example, assume you created an epoll descriptor and added to
   * there a listen-socket and a few client-sockets. Now if you will call
   * poll() on the epoll's descriptor, then on return from poll() you can
   * be sure epoll_wait() can return something useful for some of those
   * sockets.
   */
#endif
  (void)server;
  return -1;
}

int chat_server_get_socket(const struct chat_server *server) {
  return server->socket;
}

int chat_server_get_events(const struct chat_server *server) {
  if (server->socket == -1) return 0;

  int events = CHAT_EVENT_INPUT;  // Всегда ожидаем входящих данных

  // Проверяем, есть ли данные в буфере вывода у любого клиента
  for (size_t i = 0; i < server->peer_count; i++) {
    if (server->peers[i].output_size > 0) {
      events |= CHAT_EVENT_OUTPUT;
      break;  // Достаточно найти одного клиента с данными
    }
  }

  return events;
}

int chat_server_feed(struct chat_server *server, const char *msg,
                     uint32_t msg_size) {
#if NEED_SERVER_FEED
  /* IMPLEMENT THIS FUNCTION if want +5 points. */
#endif
  (void)server;
  (void)msg;
  (void)msg_size;
  return CHAT_ERR_NOT_IMPLEMENTED;
}
