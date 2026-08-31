#include "actor_runtime.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    MAF_WIRE_MESSAGE_TAG = 8701
};

typedef struct {
    int kind;
    MpiActorId sender;
    MpiActorId recipient;
    int payload_size;
    unsigned char payload[MAF_MAX_MESSAGE_PAYLOAD];
} MpiActorWireMessage;

struct MpiActorPendingSend {
    MPI_Request request;
    MpiActorWireMessage packet;
    MpiActorPendingSend *next;
};

static void maf_abort(MpiActorRuntime *runtime, const char *message) {
    fprintf(stderr, "%s\n", message);
    fflush(stderr);
    MPI_Abort(runtime->comm, EXIT_FAILURE);
}

static void maf_free_message_chain(MpiActorMessage *message) {
    while (message) {
        MpiActorMessage *next = message->next;
        free(message);
        message = next;
    }
}

static void maf_push_mailbox(MpiActor *actor, MpiActorMessage *message) {
    message->next = NULL;

    if (actor->mailbox_tail) {
        actor->mailbox_tail->next = message;
        actor->mailbox_tail = message;
    } else {
        actor->mailbox_head = message;
        actor->mailbox_tail = message;
    }
}

static MpiActorMessage *maf_pop_mailbox(MpiActor *actor) {
    MpiActorMessage *message = actor->mailbox_head;

    if (!message) {
        return NULL;
    }

    actor->mailbox_head = message->next;
    if (!actor->mailbox_head) {
        actor->mailbox_tail = NULL;
    }

    message->next = NULL;
    return message;
}

static MpiActorMessage *maf_clone_message(const MpiActorWireMessage *packet) {
    MpiActorMessage *message = calloc(1, sizeof(*message));

    if (!message) {
        return NULL;
    }

    message->kind = packet->kind;
    message->sender = packet->sender;
    message->recipient = packet->recipient;
    message->payload_size = (size_t) packet->payload_size;

    if (packet->payload_size > 0) {
        memcpy(message->payload, packet->payload, (size_t) packet->payload_size);
    }

    return message;
}

static void maf_ensure_capacity(MpiActorRuntime *runtime, MpiActorId id) {
    if ((size_t) id < runtime->actor_capacity) {
        return;
    }

    size_t new_capacity = runtime->actor_capacity ? runtime->actor_capacity : 64;
    while ((size_t) id >= new_capacity) {
        new_capacity *= 2;
    }

    MpiActor **new_local_actors = realloc(runtime->local_actors,
                                          new_capacity * sizeof(*new_local_actors));
    int *new_owner_ranks = realloc(runtime->actor_owner_ranks,
                                   new_capacity * sizeof(*new_owner_ranks));

    if (!new_local_actors || !new_owner_ranks) {
        free(new_local_actors);
        free(new_owner_ranks);
        maf_abort(runtime, "Failed to grow MPI actor registry.");
    }

    for (size_t i = runtime->actor_capacity; i < new_capacity; i++) {
        new_local_actors[i] = NULL;
        new_owner_ranks[i] = -1;
    }

    runtime->local_actors = new_local_actors;
    runtime->actor_owner_ranks = new_owner_ranks;
    runtime->actor_capacity = new_capacity;
}

static void maf_destroy_local_actor(MpiActorRuntime *runtime, MpiActorId id) {
    MpiActor *actor = maf_get_local_actor(runtime, id);

    if (!actor) {
        return;
    }

    if (actor->type && actor->type->on_destroy) {
        actor->type->on_destroy(runtime, actor);
    }

    maf_free_message_chain(actor->mailbox_head);
    free(actor->state);
    runtime->local_actors[id] = NULL;
    free(actor);
}

static bool maf_reap_pending_sends(MpiActorRuntime *runtime) {
    bool progress = false;
    MpiActorPendingSend **cursor = &runtime->pending_sends;

    while (*cursor) {
        int completed = 0;
        MpiActorPendingSend *send = *cursor;

        MPI_Test(&send->request, &completed, MPI_STATUS_IGNORE);

        if (completed) {
            *cursor = send->next;
            free(send);
            progress = true;
            continue;
        }

        cursor = &send->next;
    }

    return progress;
}

static bool maf_poll_incoming(MpiActorRuntime *runtime) {
    bool progress = false;
    int available = 0;

    MPI_Iprobe(MPI_ANY_SOURCE, MAF_WIRE_MESSAGE_TAG, runtime->comm, &available, MPI_STATUS_IGNORE);

    while (available) {
        MpiActorWireMessage packet;
        MpiActorMessage *message;
        MpiActor *recipient;

        MPI_Recv(&packet,
                 (int) sizeof(packet),
                 MPI_BYTE,
                 MPI_ANY_SOURCE,
                 MAF_WIRE_MESSAGE_TAG,
                 runtime->comm,
                 MPI_STATUS_IGNORE);

        recipient = maf_get_local_actor(runtime, packet.recipient);
        if (recipient && recipient->alive && !recipient->terminate_requested) {
            message = maf_clone_message(&packet);
            if (!message) {
                maf_abort(runtime, "Failed to allocate inbound actor message.");
            }
            maf_push_mailbox(recipient, message);
        }

        progress = true;
        MPI_Iprobe(MPI_ANY_SOURCE, MAF_WIRE_MESSAGE_TAG, runtime->comm, &available, MPI_STATUS_IGNORE);
    }

    return progress;
}

static bool maf_has_local_pending_work(MpiActorRuntime *runtime) {
    if (runtime->pending_sends) {
        return true;
    }

    for (size_t i = 0; i < runtime->actor_capacity; i++) {
        MpiActor *actor = runtime->local_actors[i];

        if (!actor || !actor->alive) {
            continue;
        }

        if (actor->mailbox_head) {
            return true;
        }
    }

    return false;
}

static void maf_collective_state(MpiActorRuntime *runtime,
                                 bool *global_pending_work,
                                 bool *global_stop_requested) {
    int local_state[2];
    int global_state[2];

    local_state[0] = maf_has_local_pending_work(runtime) ? 1 : 0;
    local_state[1] = runtime->stop_requested ? 1 : 0;

    MPI_Allreduce(local_state, global_state, 2, MPI_INT, MPI_LOR, runtime->comm);

    *global_pending_work = (global_state[0] != 0);
    *global_stop_requested = (global_state[1] != 0);
}

/*
 * A rank can appear locally idle while a remote message is still in flight.
 *
 * Before the clock is allowed to advance the model from an idle hook, force a
 * global rendezvous and then poll again so inter-rank traffic is visible in the
 * recipient mailboxes. Without this settle step, a distributed run can start
 * the next phase before migrations from the previous phase have arrived.
 */
static bool maf_settle_cluster(MpiActorRuntime *runtime) {
    bool progress = false;

    MPI_Barrier(runtime->comm);
    progress |= maf_poll_incoming(runtime);
    progress |= maf_reap_pending_sends(runtime);

    return progress;
}

static bool maf_dispatch_once(MpiActorRuntime *runtime) {
    bool progress = false;

    for (size_t i = 0; i < runtime->actor_capacity; i++) {
        MpiActor *actor = runtime->local_actors[i];

        if (!actor || !actor->alive || !actor->mailbox_head) {
            continue;
        }

        MpiActorMessage *message = maf_pop_mailbox(actor);
        actor->type->on_message(runtime, actor, message);
        free(message);
        progress = true;

        if (actor->terminate_requested) {
            maf_destroy_local_actor(runtime, actor->id);
        }
    }

    return progress;
}

static bool maf_run_idle_hooks(MpiActorRuntime *runtime) {
    bool progress = false;

    for (size_t i = 0; i < runtime->actor_capacity; i++) {
        MpiActor *actor = runtime->local_actors[i];

        if (!actor || !actor->alive || actor->mailbox_head || !actor->type->on_idle) {
            continue;
        }

        if (actor->type->on_idle(runtime, actor)) {
            progress = true;
        }

        if (actor->terminate_requested) {
            maf_destroy_local_actor(runtime, actor->id);
        }
    }

    return progress;
}

void maf_init(MpiActorRuntime *runtime, MPI_Comm comm, void *app_context) {
    memset(runtime, 0, sizeof(*runtime));

    MPI_Comm_dup(comm, &runtime->comm);
    MPI_Comm_rank(runtime->comm, &runtime->rank);
    MPI_Comm_size(runtime->comm, &runtime->world_size);

    runtime->actor_capacity = 64;
    runtime->local_actors = calloc(runtime->actor_capacity, sizeof(*runtime->local_actors));
    runtime->actor_owner_ranks = malloc(runtime->actor_capacity * sizeof(*runtime->actor_owner_ranks));
    runtime->next_actor_id = 0;
    runtime->pending_sends = NULL;
    runtime->app_context = app_context;
    runtime->stop_requested = false;

    if (!runtime->local_actors || !runtime->actor_owner_ranks) {
        maf_abort(runtime, "Failed to allocate MPI actor runtime.");
    }

    for (size_t i = 0; i < runtime->actor_capacity; i++) {
        runtime->actor_owner_ranks[i] = -1;
    }
}

void maf_destroy(MpiActorRuntime *runtime) {
    for (size_t i = 0; i < runtime->actor_capacity; i++) {
        if (runtime->local_actors[i]) {
            maf_destroy_local_actor(runtime, (MpiActorId) i);
        }
    }

    while (runtime->pending_sends) {
        MpiActorPendingSend *send = runtime->pending_sends;
        runtime->pending_sends = send->next;
        MPI_Wait(&send->request, MPI_STATUS_IGNORE);
        free(send);
    }

    free(runtime->local_actors);
    free(runtime->actor_owner_ranks);
    MPI_Comm_free(&runtime->comm);
}

void *maf_app_context(MpiActorRuntime *runtime) {
    return runtime->app_context;
}

int maf_rank(MpiActorRuntime *runtime) {
    return runtime->rank;
}

int maf_world_size(MpiActorRuntime *runtime) {
    return runtime->world_size;
}

MpiActor *maf_get_local_actor(MpiActorRuntime *runtime, MpiActorId id) {
    if (id < 0 || (size_t) id >= runtime->actor_capacity) {
        return NULL;
    }

    return runtime->local_actors[id];
}

int maf_actor_owner_rank(MpiActorRuntime *runtime, MpiActorId id) {
    if (id < 0 || (size_t) id >= runtime->actor_capacity) {
        return -1;
    }

    return runtime->actor_owner_ranks[id];
}

MpiActorId maf_spawn_actor_collective(MpiActorRuntime *runtime,
                                      int owner_rank,
                                      const MpiActorType *type,
                                      const void *initial_state,
                                      size_t initial_state_size) {
    MpiActorId id = runtime->next_actor_id++;

    if (owner_rank < 0 || owner_rank >= runtime->world_size) {
        maf_abort(runtime, "Invalid owner rank for collective actor spawn.");
    }

    maf_ensure_capacity(runtime, id);
    runtime->actor_owner_ranks[id] = owner_rank;

    if (runtime->rank == owner_rank) {
        MpiActor *actor = calloc(1, sizeof(*actor));

        if (!actor) {
            maf_abort(runtime, "Failed to allocate local actor.");
        }

        actor->id = id;
        actor->owner_rank = owner_rank;
        actor->alive = true;
        actor->terminate_requested = false;
        actor->type = type;
        actor->state_size = initial_state_size;

        if (initial_state_size > 0) {
            actor->state = calloc(1, initial_state_size);
            if (!actor->state) {
                free(actor);
                maf_abort(runtime, "Failed to allocate actor state.");
            }
            memcpy(actor->state, initial_state, initial_state_size);
        }

        runtime->local_actors[id] = actor;
    }

    return id;
}

void maf_send(MpiActorRuntime *runtime,
              MpiActorId sender,
              MpiActorId recipient,
              int kind,
              const void *payload,
              size_t payload_size) {
    int target_rank = maf_actor_owner_rank(runtime, recipient);

    if (payload_size > MAF_MAX_MESSAGE_PAYLOAD) {
        maf_abort(runtime, "Actor message payload exceeds framework limit.");
    }

    if (target_rank < 0) {
        maf_abort(runtime, "Attempted to send to an unknown actor id.");
    }

    if (target_rank == runtime->rank) {
        MpiActor *recipient_actor = maf_get_local_actor(runtime, recipient);

        if (recipient_actor && recipient_actor->alive && !recipient_actor->terminate_requested) {
            MpiActorMessage *message = calloc(1, sizeof(*message));

            if (!message) {
                maf_abort(runtime, "Failed to allocate local actor message.");
            }

            message->kind = kind;
            message->sender = sender;
            message->recipient = recipient;
            message->payload_size = payload_size;

            if (payload && payload_size > 0) {
                memcpy(message->payload, payload, payload_size);
            }

            maf_push_mailbox(recipient_actor, message);
        }

        return;
    }

    MpiActorPendingSend *send = calloc(1, sizeof(*send));

    if (!send) {
        maf_abort(runtime, "Failed to allocate outbound MPI send.");
    }

    send->packet.kind = kind;
    send->packet.sender = sender;
    send->packet.recipient = recipient;
    send->packet.payload_size = (int) payload_size;

    if (payload && payload_size > 0) {
        memcpy(send->packet.payload, payload, payload_size);
    }

    MPI_Isend(&send->packet,
              (int) sizeof(send->packet),
              MPI_BYTE,
              target_rank,
              MAF_WIRE_MESSAGE_TAG,
              runtime->comm,
              &send->request);

    send->next = runtime->pending_sends;
    runtime->pending_sends = send;
}

void maf_send_simple(MpiActorRuntime *runtime,
                     MpiActorId sender,
                     MpiActorId recipient,
                     int kind) {
    maf_send(runtime, sender, recipient, kind, NULL, 0);
}

void maf_request_actor_death(MpiActor *actor) {
    actor->alive = false;
    actor->terminate_requested = true;
}

void maf_request_stop(MpiActorRuntime *runtime) {
    runtime->stop_requested = true;
}

void maf_run(MpiActorRuntime *runtime) {
    for (;;) {
        bool progress = false;
        bool global_pending_work = false;
        bool global_stop_requested = false;

        progress |= maf_poll_incoming(runtime);
        progress |= maf_reap_pending_sends(runtime);
        progress |= maf_dispatch_once(runtime);
        progress |= maf_poll_incoming(runtime);
        progress |= maf_reap_pending_sends(runtime);

        maf_collective_state(runtime, &global_pending_work, &global_stop_requested);

        if (!global_pending_work && !global_stop_requested) {
            progress |= maf_settle_cluster(runtime);
            maf_collective_state(runtime, &global_pending_work, &global_stop_requested);
        }

        if (!global_pending_work && !global_stop_requested) {
            progress |= maf_run_idle_hooks(runtime);
            progress |= maf_poll_incoming(runtime);
            progress |= maf_reap_pending_sends(runtime);
            maf_collective_state(runtime, &global_pending_work, &global_stop_requested);
        }

        if (global_stop_requested) {
            break;
        }

        if (!global_pending_work && !progress) {
            break;
        }
    }
}
