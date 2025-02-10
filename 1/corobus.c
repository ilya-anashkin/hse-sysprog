#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

struct data_vector {
	unsigned* data;
	size_t size;
	size_t capacity;
};

#if 1 /* Uncomment this if want to use */

/** Append @a count messages in @a data to the end of the vector. */
static void
data_vector_append_many(struct data_vector* vector,
	const unsigned* data, size_t count)
{
	if (vector->size + count > vector->capacity) {
		if (vector->capacity == 0)
			vector->capacity = 4;
		else
			vector->capacity *= 2;
		if (vector->capacity < vector->size + count)
			vector->capacity = vector->size + count;
		vector->data = realloc(vector->data,
			sizeof(vector->data[0]) * vector->capacity);
	}
	memcpy(&vector->data[vector->size], data, sizeof(data[0]) * count);
	vector->size += count;
}

/** Append a single message to the vector. */
static void
data_vector_append(struct data_vector* vector, unsigned data)
{
	data_vector_append_many(vector, &data, 1);
}

/** Pop @a count of messages into @a data from the head of the vector. */
static void
data_vector_pop_first_many(struct data_vector* vector, unsigned* data, size_t count)
{
	assert(count <= vector->size);
	memcpy(data, vector->data, sizeof(data[0]) * count);
	vector->size -= count;
	memmove(vector->data, &vector->data[count], vector->size * sizeof(vector->data[0]));
}

/** Pop a single message from the head of the vector. */
// static unsigned
// data_vector_pop_first(struct data_vector* vector)
// {
// 	unsigned data = 0;
// 	data_vector_pop_first_many(vector, &data, 1);
// 	return data;
// }

#endif

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro* coro;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

#if 1 /* Uncomment this if want to use */

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_suspend_this(struct wakeup_queue* queue)
{
	struct wakeup_entry entry;
	entry.coro = coro_this();
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	rlist_del_entry(&entry, base);
}

/** Wakeup the first coroutine in the queue. */
static void
wakeup_queue_wakeup_first(struct wakeup_queue* queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry* entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	coro_wakeup(entry->coro);
}

#endif

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	struct wakeup_queue send_queue;
	/** Coroutines waiting until the channel is not empty. */
	struct wakeup_queue recv_queue;
	/** Message queue. */
	struct data_vector data;
};

struct coro_bus {
	struct coro_bus_channel** channels;
	int channel_count;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
	coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

struct coro_bus*
	coro_bus_new(void)
{
	struct coro_bus* bus = malloc(sizeof(struct coro_bus));
	bus->channels = NULL;
	bus->channel_count = 0;

	return bus;
}

void
coro_bus_delete(struct coro_bus* bus)
{
	if (!bus) {
		return;
	}

	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i]) {
			coro_bus_channel_close(bus, i);
		}
	}

	free(bus->channels);
	free(bus);
}

/** Helper function: check channel exists in coro bus. */
static bool
coro_bus_check_channel_exists(const struct coro_bus* bus, int channel)
{
	if (!bus || channel < 0 || channel >= bus->channel_count) {
		return false;
	}

	return bus->channels[channel];
}

/** Helper function: check channel size equals to limit. */
static bool
coro_bus_check_channel_size_is_limited(const struct coro_bus* bus, int channel)
{
	return bus->channels[channel]->data.size == bus->channels[channel]->size_limit;
}

int
coro_bus_channel_open(struct coro_bus* bus, size_t size_limit)
{
	struct coro_bus_channel* channel = malloc(sizeof(struct coro_bus_channel));
	channel->size_limit = size_limit;
	channel->data.data = NULL;
	channel->data.size = 0;
	channel->data.capacity = 0;
	rlist_create(&channel->send_queue.coros);
	rlist_create(&channel->recv_queue.coros);

	/* Find free channel idx */
	int free_channel_idx = -1;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (!bus->channels[i]) {
			free_channel_idx = i;
		}
	}

	/* Add new channel if free not found */
	if (free_channel_idx == -1) {
		bus->channels = realloc(bus->channels, sizeof(struct coro_bus_channel*) * (bus->channel_count + 1));
		free_channel_idx = bus->channel_count;
		bus->channel_count++;
	}

	bus->channels[free_channel_idx] = channel;
	return free_channel_idx;
}

void
coro_bus_channel_close(struct coro_bus* bus, int channel)
{
	struct coro_bus_channel* ch = bus->channels[channel];
	bus->channels[channel] = NULL;
	coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);

	struct wakeup_entry* recv_entry;
	rlist_foreach_entry(recv_entry, &ch->recv_queue.coros, base) {
		coro_wakeup(recv_entry->coro);
	}

	struct wakeup_entry* send_entry;
	rlist_foreach_entry(send_entry, &ch->send_queue.coros, base) {
		coro_wakeup(send_entry->coro);
	}
	coro_yield();

	free(ch->data.data);
	free(ch);
}

int
coro_bus_send(struct coro_bus* bus, int channel, unsigned data)
{
	int send_count = coro_bus_send_v(bus, channel, &data, 1);
	if (send_count > 0) {
		return 0;
	}

	return send_count;
}

int
coro_bus_try_send(struct coro_bus* bus, int channel, unsigned data)
{
	int try_send_count = coro_bus_try_send_v(bus, channel, &data, 1);
	if (try_send_count > 0) {
		return 0;
	}

	return try_send_count;
}

int
coro_bus_recv(struct coro_bus* bus, int channel, unsigned* data)
{
	int recv_count = coro_bus_recv_v(bus, channel, data, 1);
	if (recv_count > 0) {
		return 0;
	}

	return recv_count;
}

int
coro_bus_try_recv(struct coro_bus* bus, int channel, unsigned* data)
{
	int try_recv_count = coro_bus_try_recv_v(bus, channel, data, 1);
	if (try_recv_count > 0) {
		return 0;
	}

	return try_recv_count;
}


#if NEED_BROADCAST

int
coro_bus_broadcast(struct coro_bus* bus, unsigned data)
{
	int broadcast_result = 0;
	while (true) {
		broadcast_result = coro_bus_try_broadcast(bus, data);
		if (broadcast_result == 0) {
			break;
		}

		if (coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			for (int i = 0; i < bus->channel_count; ++i) {
				if (bus->channels[i] && bus->channels[i]->data.size == bus->channels[i]->size_limit) {
					wakeup_queue_suspend_this(&bus->channels[i]->send_queue);
					break;
				}
			}

			continue;
		}

		return broadcast_result;
	}

	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] && coro_bus_check_channel_size_is_limited(bus, i)) {
			wakeup_queue_wakeup_first(&bus->channels[i]->send_queue);
		}
	}

	return broadcast_result;
}

int
coro_bus_try_broadcast(struct coro_bus* bus, unsigned data)
{
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] && coro_bus_check_channel_size_is_limited(bus, i)) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}

	bool was_broadcasted = false;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i]) {
			was_broadcasted = true;
			data_vector_append(&bus->channels[i]->data, data);
			wakeup_queue_wakeup_first(&bus->channels[i]->recv_queue);
		}
	}

	return was_broadcasted ? 0 : -1;
}

#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus* bus, int channel, const unsigned* data, unsigned count)
{
	int send_count = 0;
	while (true) {
		send_count = coro_bus_try_send_v(bus, channel, data, count);
		if (send_count != -1) {
			break;
		}

		if (coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			wakeup_queue_suspend_this(&bus->channels[channel]->send_queue);
			continue;
		}

		return send_count;
	}

	if (bus->channels[channel]->data.size < bus->channels[channel]->size_limit) {
		wakeup_queue_wakeup_first(&bus->channels[channel]->send_queue);
	}

	return send_count;
}

int
coro_bus_try_send_v(struct coro_bus* bus, int channel, const unsigned* data, unsigned count)
{
	if (!coro_bus_check_channel_exists(bus, channel)) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	if (coro_bus_check_channel_size_is_limited(bus, channel)) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	size_t available_size = bus->channels[channel]->size_limit - bus->channels[channel]->data.size;
	int try_send_count = count > available_size ? available_size : count;

	data_vector_append_many(&bus->channels[channel]->data, data, try_send_count);
	wakeup_queue_wakeup_first(&bus->channels[channel]->recv_queue);

	return try_send_count;
}

int
coro_bus_recv_v(struct coro_bus* bus, int channel, unsigned* data, unsigned capacity)
{
	int recv_count = 0;
	while (true) {
		recv_count = coro_bus_try_recv_v(bus, channel, data, capacity);
		if (recv_count != -1) {
			break;
		}

		if (coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			wakeup_queue_suspend_this(&bus->channels[channel]->recv_queue);
			continue;
		}

		return recv_count;
	}

	if (bus->channels[channel]->data.size > 0) {
		wakeup_queue_wakeup_first(&bus->channels[channel]->recv_queue);
	}

	return recv_count;
}

int
coro_bus_try_recv_v(struct coro_bus* bus, int channel, unsigned* data, unsigned capacity)
{
	if (!coro_bus_check_channel_exists(bus, channel)) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	if (bus->channels[channel]->data.size == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	size_t recv_count = bus->channels[channel]->data.size;
	if (bus->channels[channel]->data.size > capacity) {
		recv_count = capacity;
	}

	data_vector_pop_first_many(&bus->channels[channel]->data, data, recv_count);
	wakeup_queue_wakeup_first(&bus->channels[channel]->send_queue);

	return recv_count;
}

#endif
