#ifndef MPI_ACTOR_FRAMEWORK_H
#define MPI_ACTOR_FRAMEWORK_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <mpi.h>

#define MAF_MAX_MESSAGE_PAYLOAD 128
#define MAF_INVALID_ACTOR_ID (-1)

typedef int MpiActorId;

typedef struct MpiActorRuntime MpiActorRuntime;
typedef struct MpiActor MpiActor;
typedef struct MpiActorMessage MpiActorMessage;
typedef struct MpiActorPendingSend MpiActorPendingSend;

typedef struct {
    double run_wall_seconds;
    double poll_incoming_seconds;
    double reap_pending_sends_seconds;
    double dispatch_seconds;
    double idle_hook_seconds;
    uint64_t poll_incoming_calls;
    uint64_t reap_pending_sends_calls;
    uint64_t dispatch_calls;
    uint64_t idle_hook_calls;
    uint64_t messages_dispatched;
    uint64_t local_messages_sent;
    uint64_t remote_messages_sent;
} MpiActorValidationMetrics;

typedef void (*MpiActorOnMessageFn)(MpiActorRuntime *runtime,
                                    MpiActor *self,
                                    const MpiActorMessage *message);
typedef bool (*MpiActorOnIdleFn)(MpiActorRuntime *runtime, MpiActor *self);
typedef void (*MpiActorOnDestroyFn)(MpiActorRuntime *runtime, MpiActor *self);

typedef struct {
    const char *name;
    MpiActorOnMessageFn on_message;
    MpiActorOnIdleFn on_idle;
    MpiActorOnDestroyFn on_destroy;
} MpiActorType;

struct MpiActorMessage {
    int kind;
    MpiActorId sender;
    MpiActorId recipient;
    size_t payload_size;
    unsigned char payload[MAF_MAX_MESSAGE_PAYLOAD];
    MpiActorMessage *next;
};

struct MpiActor {
    MpiActorId id;
    int owner_rank;
    bool alive;
    bool terminate_requested;
    const MpiActorType *type;
    void *state;
    size_t state_size;
    MpiActorMessage *mailbox_head;
    MpiActorMessage *mailbox_tail;
};

struct MpiActorRuntime {
    MPI_Comm comm;
    int rank;
    int world_size;

    MpiActor **local_actors;
    int *actor_owner_ranks;
    size_t actor_capacity;
    MpiActorId next_actor_id;

    MpiActorPendingSend *pending_sends;

    void *app_context;
    bool stop_requested;
    MpiActorValidationMetrics validation_metrics;
};

void maf_init(MpiActorRuntime *runtime, MPI_Comm comm, void *app_context);
void maf_destroy(MpiActorRuntime *runtime);

void *maf_app_context(MpiActorRuntime *runtime);
int maf_rank(MpiActorRuntime *runtime);
int maf_world_size(MpiActorRuntime *runtime);

MpiActor *maf_get_local_actor(MpiActorRuntime *runtime, MpiActorId id);
int maf_actor_owner_rank(MpiActorRuntime *runtime, MpiActorId id);

/*
 * Actor creation is collective across all ranks.
 *
 * Every rank must call this function in the same order with the same owner and
 * actor type. Only the owner rank allocates state, but every rank records the
 * id -> owner mapping so future messages can be routed correctly.
 */
MpiActorId maf_spawn_actor_collective(MpiActorRuntime *runtime,
                                      int owner_rank,
                                      const MpiActorType *type,
                                      const void *initial_state,
                                      size_t initial_state_size);

void maf_send(MpiActorRuntime *runtime,
              MpiActorId sender,
              MpiActorId recipient,
              int kind,
              const void *payload,
              size_t payload_size);
void maf_send_simple(MpiActorRuntime *runtime,
                     MpiActorId sender,
                     MpiActorId recipient,
                     int kind);

void maf_request_actor_death(MpiActor *actor);
void maf_request_stop(MpiActorRuntime *runtime);
void maf_run(MpiActorRuntime *runtime);
const MpiActorValidationMetrics *maf_validation_metrics(const MpiActorRuntime *runtime);

#endif
