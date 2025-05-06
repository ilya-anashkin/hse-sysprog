#include "chat.h"

#include <poll.h>
#include <stdio.h>  // For debugging
#include <stdlib.h>

void chat_message_delete(struct chat_message *msg) {
  //   printf("[TEST] exists msg: %d, exists data: %d\n", msg != NULL,
  //          msg->data != NULL);
  //   printf("Deleting message: %p, data: %p\n", msg, msg->data);
  free(msg->data);
  free(msg);
}

int chat_events_to_poll_events(int mask) {
  int res = 0;
  if ((mask & CHAT_EVENT_INPUT) != 0) res |= POLLIN;
  if ((mask & CHAT_EVENT_OUTPUT) != 0) res |= POLLOUT;
  return res;
}
