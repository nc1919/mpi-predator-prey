#include <stdbool.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "actor_model_fast_validate.h"

#ifndef PP_GRID_SIZE
#define PP_GRID_SIZE 20
#endif

#ifndef PP_MAX_PER_CELL
#define PP_MAX_PER_CELL 100
#endif

#ifndef PP_INITIAL_PREY
#define PP_INITIAL_PREY 400
#endif

#ifndef PP_INITIAL_PREDATORS
#define PP_INITIAL_PREDATORS 100
#endif

#ifndef PP_STEPS_PER_DAY
#define PP_STEPS_PER_DAY 50
#endif

#ifndef PP_DAYS
#define PP_DAYS 20
#endif

#ifndef PP_ALPHA
#define PP_ALPHA 0.06
#endif

#ifndef PP_BETA
#define PP_BETA 0.01
#endif

#ifndef PP_DELTA
#define PP_DELTA 0.10
#endif

#ifndef PP_GAMMA
#define PP_GAMMA 0.04
#endif

#ifndef PP_REGION_SIDE
#define PP_REGION_SIDE 5
#endif

#ifndef PP_SEED
#define PP_SEED 123456u
#endif

#if (PP_GRID_SIZE % PP_REGION_SIDE) != 0
#error "PP_GRID_SIZE must be divisible by PP_REGION_SIDE"
#endif

#define PP_REGION_COLS (PP_GRID_SIZE / PP_REGION_SIDE)
#define PP_REGION_ROWS (PP_GRID_SIZE / PP_REGION_SIDE)
#define PP_REGION_COUNT (PP_REGION_COLS * PP_REGION_ROWS)

typedef enum {
    PP_SPECIES_PREY,
    PP_SPECIES_PREDATOR
} PPSpecies;

typedef enum {
    PP_MSG_PHASE_MOVE_PREDATORS = 100,
    PP_MSG_PHASE_MOVE_PREY,
    PP_MSG_PHASE_PREDATION,
    PP_MSG_PHASE_DEATH,
    PP_MSG_PHASE_REPRODUCTION,
    PP_MSG_BEGIN_DAY_REPORT,
    PP_MSG_BEGIN_STEP_REPORT,
    PP_MSG_REQUEST_REGION_COUNTS,
    PP_MSG_REGION_COUNTS,
    PP_MSG_DAY_REPORTED,
    PP_MSG_STEP_REPORTED,
    PP_MSG_REGION_PHASE_DONE,
    PP_MSG_MIGRATION_REQUEST,
    PP_MSG_MIGRATION_ACCEPTED,
    PP_MSG_MIGRATION_DENIED,
    PP_MSG_EXTINCTION,
    PP_MSG_STOP
} PPMessageKind;

typedef enum {
    PP_CLOCK_PHASE_MOVE_PREDATORS,
    PP_CLOCK_PHASE_MOVE_PREY,
    PP_CLOCK_PHASE_PREDATION,
    PP_CLOCK_PHASE_DEATH,
    PP_CLOCK_PHASE_REPRODUCTION,
    PP_CLOCK_PHASE_ADVANCE
} PPClockPhase;

typedef struct {
    uint64_t animal_id;
    PPSpecies species;
    int x;
    int y;
    bool alive;
    bool in_flight;
} PPAnimal;

typedef struct {
    int day;
    int step;
} PPReportPayload;

typedef struct {
    int day;
    int step;
    int prey_count;
    int predator_count;
} PPRegionCountsPayload;

typedef struct {
    int day;
    int step;
    PPMessageKind phase_kind;
} PPPhasePayload;

typedef struct {
    uint64_t animal_id;
    PPSpecies species;
    int old_x;
    int old_y;
    int new_x;
    int new_y;
    MpiActorId source_region_actor;
} PPMigrationRequestPayload;

typedef struct {
    uint64_t animal_id;
} PPMigrationDecisionPayload;

typedef struct {
    int day;
    int step;
    PPClockPhase next_phase;
    PPMessageKind active_phase_kind;
    bool waiting_for_report;
    bool waiting_for_phase_completion;
    bool pending_day_advance;
    bool finished;
    int completed_regions;
} PPClockState;

typedef struct {
    int report_day;
    int report_step;
    int reports_received;
    int prey_total;
    int predator_total;
    bool report_is_day;
    bool extinction_announced;
    MpiActorId clock_actor;
} PPStatsState;

typedef struct {
    int region_index;
    int start_x;
    int start_y;
    int width;
    int height;
    unsigned int rng_state;
    PPAnimal *animals;
    int animal_count;
    int animal_capacity;
    uint32_t next_local_animal_serial;
    PPMessageKind active_phase_kind;
    int phase_day;
    int phase_step;
    int pending_migration_responses;
    bool phase_done_sent;
} PPRegionState;

typedef struct {
    MpiActorId clock_actor;
    MpiActorId stats_actor;
    MpiActorId region_actors[PP_REGION_COUNT];
    unsigned int bootstrap_rng;
    double run_start_seconds;
    double current_day_start_seconds;
    double current_step_start_seconds;
    double total_step_seconds;
    double total_day_seconds;
    double move_decision_seconds;
    double occupancy_seconds;
    double rng_seconds;
    double allocation_seconds;
    uint64_t completed_steps;
    uint64_t completed_days;
    uint64_t occupancy_calls;
    uint64_t rng_calls;
    uint64_t allocation_events;
    uint64_t occupancy_violations;
    uint64_t duplicate_id_violations;
    uint64_t migration_requests;
    uint64_t migration_accepted;
    uint64_t migration_denied;
    int validation_stride_days;
    int next_validation_day;
} PredatorPreyModel;

static PredatorPreyModel *pp_validation_model = NULL;

static void pp_abort(const char *message) {
    fprintf(stderr, "%s\n", message);
    fflush(stderr);
    MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
}

static double pp_now_seconds(void) {
    return MPI_Wtime();
}

static unsigned int pp_runtime_seed(void) {
    const char *seed_env = getenv("PREDPREY_RUNTIME_SEED");
    char *end = NULL;
    unsigned long value;

    if (!seed_env || seed_env[0] == '\0') {
        return PP_SEED;
    }

    value = strtoul(seed_env, &end, 10);
    if (!end || *end != '\0') {
        pp_abort("Invalid PREDPREY_RUNTIME_SEED value.");
    }

    return (unsigned int) value;
}

static int pp_validation_stride_days(void) {
    const char *stride_env = getenv("PREDPREY_VALIDATE_STRIDE_DAYS");
    char *end = NULL;
    long value;

    if (!stride_env || stride_env[0] == '\0') {
        return 1;
    }

    value = strtol(stride_env, &end, 10);
    if (!end || *end != '\0' || value < 0) {
        pp_abort("Invalid PREDPREY_VALIDATE_STRIDE_DAYS value.");
    }

    return (int) value;
}

static unsigned int pp_rng_next(unsigned int *state) {
    double start_time = pp_now_seconds();

    *state = (*state * 1664525u) + 1013904223u;

    if (pp_validation_model) {
        pp_validation_model->rng_calls++;
        pp_validation_model->rng_seconds += pp_now_seconds() - start_time;
    }

    return *state;
}

static double pp_rng_unit(unsigned int *state) {
    return pp_rng_next(state) / ((double) UINT32_MAX + 1.0);
}

static int pp_rng_mod(unsigned int *state, int modulus) {
    return (int) (pp_rng_next(state) % (unsigned int) modulus);
}

static int pp_wrap_coordinate(int value) {
    if (value < 0) {
        return value + PP_GRID_SIZE;
    }

    if (value >= PP_GRID_SIZE) {
        return value - PP_GRID_SIZE;
    }

    return value;
}

static PredatorPreyModel *pp_model(MpiActorRuntime *runtime) {
    return (PredatorPreyModel *) maf_app_context(runtime);
}

static PPClockState *pp_clock_state(MpiActor *actor) {
    return (PPClockState *) actor->state;
}

static PPStatsState *pp_stats_state(MpiActor *actor) {
    return (PPStatsState *) actor->state;
}

static PPRegionState *pp_region_state(MpiActor *actor) {
    return (PPRegionState *) actor->state;
}

static int pp_region_index_from_coords(int x, int y) {
    int region_col = x / PP_REGION_SIDE;
    int region_row = y / PP_REGION_SIDE;
    return region_row * PP_REGION_COLS + region_col;
}

static int pp_owner_rank_for_region(MpiActorRuntime *runtime, int region_index) {
    return region_index % maf_world_size(runtime);
}

static uint64_t pp_region_next_animal_id(PPRegionState *region) {
    uint64_t high_bits = ((uint64_t) (uint32_t) region->region_index) << 32;
    return high_bits | (uint64_t) region->next_local_animal_serial++;
}

static void pp_region_reserve(PPRegionState *region, int required_capacity) {
    double start_time = pp_now_seconds();

    if (required_capacity <= region->animal_capacity) {
        return;
    }

    int new_capacity = region->animal_capacity ? region->animal_capacity : 64;
    while (new_capacity < required_capacity) {
        new_capacity *= 2;
    }

    PPAnimal *new_animals = realloc(region->animals, (size_t) new_capacity * sizeof(*new_animals));
    if (!new_animals) {
        pp_abort("Failed to grow region animal storage.");
    }

    region->animals = new_animals;
    region->animal_capacity = new_capacity;

    if (pp_validation_model) {
        pp_validation_model->allocation_events++;
        pp_validation_model->allocation_seconds += pp_now_seconds() - start_time;
    }
}

static void pp_region_add_animal_record(PPRegionState *region,
                                        uint64_t animal_id,
                                        PPSpecies species,
                                        int x,
                                        int y,
                                        bool in_flight) {
    pp_region_reserve(region, region->animal_count + 1);

    region->animals[region->animal_count].animal_id = animal_id;
    region->animals[region->animal_count].species = species;
    region->animals[region->animal_count].x = x;
    region->animals[region->animal_count].y = y;
    region->animals[region->animal_count].alive = true;
    region->animals[region->animal_count].in_flight = in_flight;
    region->animal_count++;
}

static void pp_region_remove_index(PPRegionState *region, int index) {
    region->animal_count--;
    region->animals[index] = region->animals[region->animal_count];
}

static int pp_region_find_animal_index(const PPRegionState *region, uint64_t animal_id) {
    for (int i = 0; i < region->animal_count; i++) {
        if (region->animals[i].animal_id == animal_id) {
            return i;
        }
    }

    return -1;
}

static void pp_region_compact_dead(PPRegionState *region) {
    int write_index = 0;

    for (int read_index = 0; read_index < region->animal_count; read_index++) {
        if (!region->animals[read_index].alive) {
            continue;
        }

        if (write_index != read_index) {
            region->animals[write_index] = region->animals[read_index];
        }

        write_index++;
    }

    region->animal_count = write_index;
}

static int pp_region_cell_occupancy(const PPRegionState *region, int x, int y) {
    double start_time = pp_now_seconds();
    int count = 0;

    for (int i = 0; i < region->animal_count; i++) {
        const PPAnimal *animal = &region->animals[i];

        /*
         * Outbound migrations remain at their source coordinates until the
         * destination explicitly accepts them. Counting in-flight animals here
         * reserves that source-cell capacity so a later denial cannot push the
         * cell above PP_MAX_PER_CELL.
         */
        if (!animal->alive) {
            continue;
        }

        if (animal->x == x && animal->y == y) {
            count++;
        }
    }

    if (pp_validation_model) {
        pp_validation_model->occupancy_calls++;
        pp_validation_model->occupancy_seconds += pp_now_seconds() - start_time;
    }

    return count;
}

static void pp_region_validate(PPRegionState *region) {
    if (!pp_validation_model) {
        return;
    }

    for (int i = 0; i < region->animal_count; i++) {
        const PPAnimal *animal = &region->animals[i];

        if (!animal->alive) {
            continue;
        }

        if (pp_region_cell_occupancy(region, animal->x, animal->y) > PP_MAX_PER_CELL) {
            pp_validation_model->occupancy_violations++;
        }

        for (int j = i + 1; j < region->animal_count; j++) {
            const PPAnimal *other = &region->animals[j];

            if (!other->alive) {
                continue;
            }

            if (animal->animal_id == other->animal_id) {
                pp_validation_model->duplicate_id_violations++;
            }
        }
    }
}

static bool pp_should_validate_day(const PredatorPreyModel *model, int day) {
    if (!model || model->validation_stride_days <= 0) {
        return false;
    }

    return day >= model->next_validation_day;
}

static void pp_validate_all_regions(MpiActorRuntime *runtime, int day) {
    PredatorPreyModel *model = pp_model(runtime);

    if (!pp_should_validate_day(model, day)) {
        return;
    }

    for (int region = 0; region < PP_REGION_COUNT; region++) {
        MpiActor *region_actor = maf_get_local_actor(runtime, model->region_actors[region]);

        if (!region_actor) {
            continue;
        }

        pp_region_validate(pp_region_state(region_actor));
    }

    model->next_validation_day = day + model->validation_stride_days;
}

static void pp_send_report_payload(MpiActorRuntime *runtime,
                                   MpiActorId sender,
                                   MpiActorId recipient,
                                   PPMessageKind kind,
                                   int day,
                                   int step) {
    PPReportPayload payload;
    payload.day = day;
    payload.step = step;
    maf_send(runtime, sender, recipient, kind, &payload, sizeof(payload));
}

static void pp_send_phase_payload(MpiActorRuntime *runtime,
                                  MpiActorId sender,
                                  MpiActorId recipient,
                                  PPMessageKind kind,
                                  int day,
                                  int step,
                                  PPMessageKind phase_kind) {
    PPPhasePayload payload;
    payload.day = day;
    payload.step = step;
    payload.phase_kind = phase_kind;
    maf_send(runtime, sender, recipient, kind, &payload, sizeof(payload));
}

static void pp_send_region_counts(MpiActorRuntime *runtime,
                                  MpiActorId sender,
                                  MpiActorId recipient,
                                  int day,
                                  int step,
                                  int prey_count,
                                  int predator_count) {
    PPRegionCountsPayload payload;
    payload.day = day;
    payload.step = step;
    payload.prey_count = prey_count;
    payload.predator_count = predator_count;
    maf_send(runtime, sender, recipient, PP_MSG_REGION_COUNTS, &payload, sizeof(payload));
}

static void pp_send_migration_request(MpiActorRuntime *runtime,
                                      MpiActorId sender,
                                      MpiActorId recipient,
                                      const PPMigrationRequestPayload *payload) {
    maf_send(runtime, sender, recipient, PP_MSG_MIGRATION_REQUEST, payload, sizeof(*payload));
}

static void pp_send_migration_decision(MpiActorRuntime *runtime,
                                       MpiActorId sender,
                                       MpiActorId recipient,
                                       PPMessageKind kind,
                                       uint64_t animal_id) {
    PPMigrationDecisionPayload payload;
    payload.animal_id = animal_id;
    maf_send(runtime, sender, recipient, kind, &payload, sizeof(payload));
}

static bool pp_is_move_phase_kind(PPMessageKind kind) {
    return kind == PP_MSG_PHASE_MOVE_PREDATORS || kind == PP_MSG_PHASE_MOVE_PREY;
}

static PPClockPhase pp_next_clock_phase(PPClockPhase phase) {
    switch (phase) {
        case PP_CLOCK_PHASE_MOVE_PREDATORS:
            return PP_CLOCK_PHASE_MOVE_PREY;
        case PP_CLOCK_PHASE_MOVE_PREY:
            return PP_CLOCK_PHASE_PREDATION;
        case PP_CLOCK_PHASE_PREDATION:
            return PP_CLOCK_PHASE_DEATH;
        case PP_CLOCK_PHASE_DEATH:
            return PP_CLOCK_PHASE_REPRODUCTION;
        case PP_CLOCK_PHASE_REPRODUCTION:
            return PP_CLOCK_PHASE_ADVANCE;
        case PP_CLOCK_PHASE_ADVANCE:
            return PP_CLOCK_PHASE_MOVE_PREDATORS;
    }

    return PP_CLOCK_PHASE_MOVE_PREDATORS;
}

static void pp_broadcast_phase(MpiActorRuntime *runtime,
                               MpiActorId sender,
                               PPMessageKind kind,
                               int day,
                               int step) {
    PredatorPreyModel *model = pp_model(runtime);

    for (int region = 0; region < PP_REGION_COUNT; region++) {
        pp_send_phase_payload(runtime,
                              sender,
                              model->region_actors[region],
                              kind,
                              day,
                              step,
                              kind);
    }
}

static void pp_broadcast_stop(MpiActorRuntime *runtime, MpiActorId sender) {
    PredatorPreyModel *model = pp_model(runtime);

    maf_send_simple(runtime, sender, model->stats_actor, PP_MSG_STOP);

    for (int region = 0; region < PP_REGION_COUNT; region++) {
        maf_send_simple(runtime, sender, model->region_actors[region], PP_MSG_STOP);
    }
}

static void pp_region_count_species(const PPRegionState *region,
                                    int *prey_count,
                                    int *predator_count) {
    *prey_count = 0;
    *predator_count = 0;

    for (int i = 0; i < region->animal_count; i++) {
        const PPAnimal *animal = &region->animals[i];

        if (!animal->alive || animal->in_flight) {
            continue;
        }

        if (animal->species == PP_SPECIES_PREY) {
            (*prey_count)++;
        } else {
            (*predator_count)++;
        }
    }
}

static void pp_region_do_move_phase(MpiActorRuntime *runtime,
                                    MpiActor *self,
                                    PPSpecies species) {
    double start_time = pp_now_seconds();
    PredatorPreyModel *model = pp_model(runtime);
    PPRegionState *region = pp_region_state(self);
    int initial_count = region->animal_count;

    for (int i = 0; i < initial_count; i++) {
        PPAnimal *animal = &region->animals[i];

        if (!animal->alive || animal->in_flight) {
            continue;
        }

        if (animal->species != species) {
            continue;
        }

        int dx = pp_rng_mod(&region->rng_state, 3) - 1;
        int dy = pp_rng_mod(&region->rng_state, 3) - 1;
        int new_x = pp_wrap_coordinate(animal->x + dx);
        int new_y = pp_wrap_coordinate(animal->y + dy);
        int destination_region = pp_region_index_from_coords(new_x, new_y);

        if (destination_region == region->region_index) {
            if (pp_region_cell_occupancy(region, new_x, new_y) < PP_MAX_PER_CELL) {
                animal->x = new_x;
                animal->y = new_y;
            }
            continue;
        }

        PPMigrationRequestPayload payload;
        payload.animal_id = animal->animal_id;
        payload.species = animal->species;
        payload.old_x = animal->x;
        payload.old_y = animal->y;
        payload.new_x = new_x;
        payload.new_y = new_y;
        payload.source_region_actor = self->id;

        animal->in_flight = true;
        region->pending_migration_responses++;
        if (pp_validation_model) {
            pp_validation_model->migration_requests++;
        }
        pp_send_migration_request(runtime,
                                  self->id,
                                  model->region_actors[destination_region],
                                  &payload);
    }

    if (pp_validation_model) {
        pp_validation_model->move_decision_seconds += pp_now_seconds() - start_time;
    }
}

static void pp_send_region_phase_done(MpiActorRuntime *runtime,
                                      MpiActor *self,
                                      PPRegionState *region) {
    region->phase_done_sent = true;
    pp_send_phase_payload(runtime,
                          self->id,
                          pp_model(runtime)->clock_actor,
                          PP_MSG_REGION_PHASE_DONE,
                          region->phase_day,
                          region->phase_step,
                          region->active_phase_kind);
}

static void pp_region_maybe_finish_phase(MpiActorRuntime *runtime,
                                         MpiActor *self,
                                         PPRegionState *region) {
    if (region->phase_done_sent) {
        return;
    }

    if (pp_is_move_phase_kind(region->active_phase_kind) &&
        region->pending_migration_responses != 0) {
        return;
    }

    pp_send_region_phase_done(runtime, self, region);
}

static void pp_region_do_predation_phase(PPRegionState *region) {
    int initial_count = region->animal_count;

    /*
     * Match the baseline more closely:
     * - iterate predators in stable local order
     * - scan prey sequentially until a same-cell prey is eaten
     * - stop after at most one prey kill per predator per step
     */
    for (int predator_index = 0; predator_index < initial_count; predator_index++) {
        PPAnimal *predator = &region->animals[predator_index];
        int predator_x;
        int predator_y;

        if (!predator->alive || predator->in_flight) {
            continue;
        }

        if (predator->species != PP_SPECIES_PREDATOR) {
            continue;
        }

        predator_x = predator->x;
        predator_y = predator->y;

        for (int prey_index = 0; prey_index < region->animal_count; prey_index++) {
            PPAnimal *prey = &region->animals[prey_index];

            if (!prey->alive || prey->in_flight) {
                continue;
            }

            if (prey->species != PP_SPECIES_PREY) {
                continue;
            }

            if (prey->x != predator_x || prey->y != predator_y) {
                continue;
            }

            if (pp_rng_unit(&region->rng_state) >= PP_BETA) {
                continue;
            }

            prey->alive = false;

            if (pp_rng_unit(&region->rng_state) < PP_DELTA &&
                pp_region_cell_occupancy(region, predator_x, predator_y) < PP_MAX_PER_CELL) {
                pp_region_add_animal_record(region,
                                            pp_region_next_animal_id(region),
                                            PP_SPECIES_PREDATOR,
                                            predator_x,
                                            predator_y,
                                            false);
            }

            break;
        }
    }
    pp_region_compact_dead(region);
}

static void pp_region_do_death_phase(PPRegionState *region) {
    for (int i = 0; i < region->animal_count; i++) {
        PPAnimal *animal = &region->animals[i];

        if (!animal->alive || animal->in_flight) {
            continue;
        }

        if (animal->species == PP_SPECIES_PREDATOR &&
            pp_rng_unit(&region->rng_state) < PP_GAMMA) {
            animal->alive = false;
        }
    }

    pp_region_compact_dead(region);
}

static void pp_region_do_reproduction_phase(PPRegionState *region) {
    int initial_count = region->animal_count;

    for (int i = 0; i < initial_count; i++) {
        PPAnimal *animal = &region->animals[i];

        if (!animal->alive || animal->in_flight) {
            continue;
        }

        if (animal->species != PP_SPECIES_PREY) {
            continue;
        }

        if (pp_rng_unit(&region->rng_state) >= PP_ALPHA) {
            continue;
        }

        if (pp_region_cell_occupancy(region, animal->x, animal->y) >= PP_MAX_PER_CELL) {
            continue;
        }

        pp_region_add_animal_record(region,
                                    pp_region_next_animal_id(region),
                                    PP_SPECIES_PREY,
                                    animal->x,
                                    animal->y,
                                    false);
    }

}

static void pp_clock_on_message(MpiActorRuntime *runtime,
                                MpiActor *self,
                                const MpiActorMessage *message) {
    PPClockState *clock = pp_clock_state(self);
    const PPReportPayload *report_payload;
    PredatorPreyModel *model = pp_model(runtime);

    switch (message->kind) {
        case PP_MSG_STEP_REPORTED:
            report_payload = (const PPReportPayload *) message->payload;
            if (clock->waiting_for_report &&
                !clock->pending_day_advance &&
                report_payload->day == clock->day &&
                report_payload->step == clock->step) {
                double now = pp_now_seconds();
                clock->waiting_for_report = false;
                model->completed_steps++;
                model->total_step_seconds += now - model->current_step_start_seconds;
                model->current_step_start_seconds = now;
                clock->step++;
            }
            break;

        case PP_MSG_DAY_REPORTED:
            report_payload = (const PPReportPayload *) message->payload;
            if (clock->waiting_for_report &&
                clock->pending_day_advance &&
                report_payload->day == clock->day &&
                report_payload->step == clock->step) {
                double now = pp_now_seconds();
                clock->waiting_for_report = false;
                clock->pending_day_advance = false;
                model->completed_steps++;
                model->completed_days++;
                model->total_step_seconds += now - model->current_step_start_seconds;
                model->total_day_seconds += now - model->current_day_start_seconds;
                model->current_step_start_seconds = now;
                model->current_day_start_seconds = now;
                clock->day++;
                clock->step = 0;
                if (clock->day >= PP_DAYS) {
                    clock->finished = true;
                }
            }
            break;

        case PP_MSG_EXTINCTION:
            clock->finished = true;
            break;

        case PP_MSG_REGION_PHASE_DONE: {
            const PPPhasePayload *payload = (const PPPhasePayload *) message->payload;

            if (!clock->waiting_for_phase_completion ||
                payload->day != clock->day ||
                payload->step != clock->step ||
                payload->phase_kind != clock->active_phase_kind) {
                break;
            }

            clock->completed_regions++;
            if (clock->completed_regions == PP_REGION_COUNT) {
                clock->waiting_for_phase_completion = false;
                clock->next_phase = pp_next_clock_phase(clock->next_phase);
            }
            break;
        }

        case PP_MSG_STOP:
            maf_request_stop(runtime);
            break;

        default:
            break;
    }
}

/*
 * Phase advancement is now explicit:
 * each region acknowledges phase completion back to the clock actor, and the
 * clock only advances once every region has reported done for the current
 * day/step/phase.
 */
static bool pp_clock_on_idle(MpiActorRuntime *runtime, MpiActor *self) {
    PPClockState *clock = pp_clock_state(self);

    if (clock->finished) {
        pp_broadcast_stop(runtime, self->id);
        maf_request_stop(runtime);
        return true;
    }

    if (clock->waiting_for_report || clock->waiting_for_phase_completion) {
        return false;
    }

    switch (clock->next_phase) {
        case PP_CLOCK_PHASE_MOVE_PREDATORS:
            clock->active_phase_kind = PP_MSG_PHASE_MOVE_PREDATORS;
            clock->completed_regions = 0;
            clock->waiting_for_phase_completion = true;
            pp_broadcast_phase(runtime, self->id, PP_MSG_PHASE_MOVE_PREDATORS, clock->day, clock->step);
            return true;

        case PP_CLOCK_PHASE_MOVE_PREY:
            clock->active_phase_kind = PP_MSG_PHASE_MOVE_PREY;
            clock->completed_regions = 0;
            clock->waiting_for_phase_completion = true;
            pp_broadcast_phase(runtime, self->id, PP_MSG_PHASE_MOVE_PREY, clock->day, clock->step);
            return true;

        case PP_CLOCK_PHASE_PREDATION:
            clock->active_phase_kind = PP_MSG_PHASE_PREDATION;
            clock->completed_regions = 0;
            clock->waiting_for_phase_completion = true;
            pp_broadcast_phase(runtime, self->id, PP_MSG_PHASE_PREDATION, clock->day, clock->step);
            return true;

        case PP_CLOCK_PHASE_DEATH:
            clock->active_phase_kind = PP_MSG_PHASE_DEATH;
            clock->completed_regions = 0;
            clock->waiting_for_phase_completion = true;
            pp_broadcast_phase(runtime, self->id, PP_MSG_PHASE_DEATH, clock->day, clock->step);
            return true;

        case PP_CLOCK_PHASE_REPRODUCTION:
            clock->active_phase_kind = PP_MSG_PHASE_REPRODUCTION;
            clock->completed_regions = 0;
            clock->waiting_for_phase_completion = true;
            pp_broadcast_phase(runtime, self->id, PP_MSG_PHASE_REPRODUCTION, clock->day, clock->step);
            return true;

        case PP_CLOCK_PHASE_ADVANCE:
            if (clock->step + 1 >= PP_STEPS_PER_DAY) {
                pp_send_report_payload(runtime,
                                       self->id,
                                       pp_model(runtime)->stats_actor,
                                       PP_MSG_BEGIN_DAY_REPORT,
                                       clock->day,
                                       clock->step);
                clock->waiting_for_report = true;
                clock->pending_day_advance = true;
            } else {
                pp_send_report_payload(runtime,
                                       self->id,
                                       pp_model(runtime)->stats_actor,
                                       PP_MSG_BEGIN_STEP_REPORT,
                                       clock->day,
                                       clock->step);
                clock->waiting_for_report = true;
                clock->pending_day_advance = false;
            }
            clock->next_phase = PP_CLOCK_PHASE_MOVE_PREDATORS;
            return true;
    }

    return false;
}

static void pp_stats_on_message(MpiActorRuntime *runtime,
                                MpiActor *self,
                                const MpiActorMessage *message) {
    PredatorPreyModel *model = pp_model(runtime);
    PPStatsState *stats = pp_stats_state(self);

    switch (message->kind) {
        case PP_MSG_BEGIN_STEP_REPORT:
        case PP_MSG_BEGIN_DAY_REPORT: {
            const PPReportPayload *payload = (const PPReportPayload *) message->payload;
            stats->report_day = payload->day;
            stats->report_step = payload->step;
            stats->reports_received = 0;
            stats->prey_total = 0;
            stats->predator_total = 0;
            stats->report_is_day = (message->kind == PP_MSG_BEGIN_DAY_REPORT);

            for (int region = 0; region < PP_REGION_COUNT; region++) {
                pp_send_report_payload(runtime,
                                       self->id,
                                       model->region_actors[region],
                                       PP_MSG_REQUEST_REGION_COUNTS,
                                       stats->report_day,
                                       stats->report_step);
            }
            break;
        }

        case PP_MSG_REGION_COUNTS: {
            const PPRegionCountsPayload *payload = (const PPRegionCountsPayload *) message->payload;

            if (payload->day != stats->report_day ||
                payload->step != stats->report_step) {
                break;
            }

            stats->reports_received++;
            stats->prey_total += payload->prey_count;
            stats->predator_total += payload->predator_count;

            if (stats->reports_received == PP_REGION_COUNT) {
                if (!stats->extinction_announced &&
                    stats->prey_total == 0 &&
                    stats->predator_total == 0) {
                    printf("All animals extinct.\n");
                    fflush(stdout);
                    stats->extinction_announced = true;
                    maf_send_simple(runtime, self->id, stats->clock_actor, PP_MSG_EXTINCTION);
                    break;
                }

                if (stats->report_is_day) {
                    pp_validate_all_regions(runtime, stats->report_day);
                    printf("%d,%d,%d\n",
                           stats->report_day,
                           stats->prey_total,
                           stats->predator_total);
                    fflush(stdout);
                    fprintf(stderr,
                            "VALIDATION_TRAJECTORY day=%d prey=%d predators=%d total=%d\n",
                            stats->report_day,
                            stats->prey_total,
                            stats->predator_total,
                            stats->prey_total + stats->predator_total);
                    fflush(stderr);

                    pp_send_report_payload(runtime,
                                           self->id,
                                           stats->clock_actor,
                                           PP_MSG_DAY_REPORTED,
                                           stats->report_day,
                                           stats->report_step);
                } else {
                    pp_send_report_payload(runtime,
                                           self->id,
                                           stats->clock_actor,
                                           PP_MSG_STEP_REPORTED,
                                           stats->report_day,
                                           stats->report_step);
                }
            }
            break;
        }

        case PP_MSG_STOP:
            maf_request_stop(runtime);
            break;

        default:
            break;
    }
}

static void pp_region_on_message(MpiActorRuntime *runtime,
                                 MpiActor *self,
                                 const MpiActorMessage *message) {
    PPRegionState *region = pp_region_state(self);

    switch (message->kind) {
        case PP_MSG_PHASE_MOVE_PREDATORS: {
            const PPPhasePayload *payload = (const PPPhasePayload *) message->payload;
            region->active_phase_kind = message->kind;
            region->phase_day = payload->day;
            region->phase_step = payload->step;
            region->pending_migration_responses = 0;
            region->phase_done_sent = false;
            pp_region_do_move_phase(runtime, self, PP_SPECIES_PREDATOR);
            pp_region_maybe_finish_phase(runtime, self, region);
            break;
        }

        case PP_MSG_PHASE_MOVE_PREY: {
            const PPPhasePayload *payload = (const PPPhasePayload *) message->payload;
            region->active_phase_kind = message->kind;
            region->phase_day = payload->day;
            region->phase_step = payload->step;
            region->pending_migration_responses = 0;
            region->phase_done_sent = false;
            pp_region_do_move_phase(runtime, self, PP_SPECIES_PREY);
            pp_region_maybe_finish_phase(runtime, self, region);
            break;
        }

        case PP_MSG_PHASE_PREDATION: {
            const PPPhasePayload *payload = (const PPPhasePayload *) message->payload;
            region->active_phase_kind = message->kind;
            region->phase_day = payload->day;
            region->phase_step = payload->step;
            region->pending_migration_responses = 0;
            region->phase_done_sent = false;
            pp_region_do_predation_phase(region);
            pp_region_maybe_finish_phase(runtime, self, region);
            break;
        }

        case PP_MSG_PHASE_DEATH: {
            const PPPhasePayload *payload = (const PPPhasePayload *) message->payload;
            region->active_phase_kind = message->kind;
            region->phase_day = payload->day;
            region->phase_step = payload->step;
            region->pending_migration_responses = 0;
            region->phase_done_sent = false;
            pp_region_do_death_phase(region);
            pp_region_maybe_finish_phase(runtime, self, region);
            break;
        }

        case PP_MSG_PHASE_REPRODUCTION: {
            const PPPhasePayload *payload = (const PPPhasePayload *) message->payload;
            region->active_phase_kind = message->kind;
            region->phase_day = payload->day;
            region->phase_step = payload->step;
            region->pending_migration_responses = 0;
            region->phase_done_sent = false;
            pp_region_do_reproduction_phase(region);
            pp_region_maybe_finish_phase(runtime, self, region);
            break;
        }

        case PP_MSG_REQUEST_REGION_COUNTS: {
            const PPReportPayload *payload = (const PPReportPayload *) message->payload;
            int prey_count;
            int predator_count;

            pp_region_count_species(region, &prey_count, &predator_count);
            pp_send_region_counts(runtime,
                                  self->id,
                                  message->sender,
                                  payload->day,
                                  payload->step,
                                  prey_count,
                                  predator_count);
            break;
        }

        case PP_MSG_MIGRATION_REQUEST: {
            const PPMigrationRequestPayload *payload = (const PPMigrationRequestPayload *) message->payload;

            if (pp_region_cell_occupancy(region, payload->new_x, payload->new_y) < PP_MAX_PER_CELL) {
                pp_region_add_animal_record(region,
                                            payload->animal_id,
                                            payload->species,
                                            payload->new_x,
                                            payload->new_y,
                                            false);
                pp_send_migration_decision(runtime,
                                           self->id,
                                           payload->source_region_actor,
                                           PP_MSG_MIGRATION_ACCEPTED,
                                           payload->animal_id);
            } else {
                pp_send_migration_decision(runtime,
                                           self->id,
                                           payload->source_region_actor,
                                           PP_MSG_MIGRATION_DENIED,
                                           payload->animal_id);
            }
            break;
        }

        case PP_MSG_MIGRATION_ACCEPTED: {
            const PPMigrationDecisionPayload *payload = (const PPMigrationDecisionPayload *) message->payload;
            int animal_index = pp_region_find_animal_index(region, payload->animal_id);

            if (animal_index >= 0) {
                pp_region_remove_index(region, animal_index);
            }
            if (pp_validation_model) {
                pp_validation_model->migration_accepted++;
            }
            if (region->pending_migration_responses > 0) {
                region->pending_migration_responses--;
            }
            pp_region_maybe_finish_phase(runtime, self, region);
            break;
        }

        case PP_MSG_MIGRATION_DENIED: {
            const PPMigrationDecisionPayload *payload = (const PPMigrationDecisionPayload *) message->payload;
            int animal_index = pp_region_find_animal_index(region, payload->animal_id);

            if (animal_index >= 0) {
                region->animals[animal_index].in_flight = false;
            }
            if (pp_validation_model) {
                pp_validation_model->migration_denied++;
            }
            if (region->pending_migration_responses > 0) {
                region->pending_migration_responses--;
            }
            pp_region_maybe_finish_phase(runtime, self, region);
            break;
        }

        case PP_MSG_STOP:
            maf_request_stop(runtime);
            break;

        default:
            break;
    }
}

static void pp_region_on_destroy(MpiActorRuntime *runtime, MpiActor *self) {
    PPRegionState *region = pp_region_state(self);

    (void) runtime;

    free(region->animals);
    region->animals = NULL;
}

static const MpiActorType PP_CLOCK_ACTOR_TYPE = {
    "pp_clock",
    pp_clock_on_message,
    pp_clock_on_idle,
    NULL
};

static const MpiActorType PP_STATS_ACTOR_TYPE = {
    "pp_stats",
    pp_stats_on_message,
    NULL,
    NULL
};

static const MpiActorType PP_REGION_ACTOR_TYPE = {
    "pp_region",
    pp_region_on_message,
    NULL,
    pp_region_on_destroy
};

static void pp_create_clock_actor(MpiActorRuntime *runtime) {
    PredatorPreyModel *model = pp_model(runtime);
    PPClockState initial_state;

    memset(&initial_state, 0, sizeof(initial_state));
    initial_state.day = 0;
    initial_state.step = 0;
    initial_state.next_phase = PP_CLOCK_PHASE_MOVE_PREDATORS;
    initial_state.active_phase_kind = PP_MSG_PHASE_MOVE_PREDATORS;
    initial_state.waiting_for_report = false;
    initial_state.waiting_for_phase_completion = false;
    initial_state.pending_day_advance = false;
    initial_state.finished = false;
    initial_state.completed_regions = 0;

    model->clock_actor = maf_spawn_actor_collective(runtime,
                                                    0,
                                                    &PP_CLOCK_ACTOR_TYPE,
                                                    &initial_state,
                                                    sizeof(initial_state));
}

static void pp_create_stats_actor(MpiActorRuntime *runtime) {
    PredatorPreyModel *model = pp_model(runtime);
    PPStatsState initial_state;

    memset(&initial_state, 0, sizeof(initial_state));
    initial_state.report_day = 0;
    initial_state.report_step = 0;
    initial_state.reports_received = 0;
    initial_state.prey_total = 0;
    initial_state.predator_total = 0;
    initial_state.report_is_day = false;
    initial_state.extinction_announced = false;
    initial_state.clock_actor = model->clock_actor;

    model->stats_actor = maf_spawn_actor_collective(runtime,
                                                    0,
                                                    &PP_STATS_ACTOR_TYPE,
                                                    &initial_state,
                                                    sizeof(initial_state));
}

static void pp_create_region_actors(MpiActorRuntime *runtime) {
    PredatorPreyModel *model = pp_model(runtime);

    for (int region_index = 0; region_index < PP_REGION_COUNT; region_index++) {
        PPRegionState initial_state;
        int region_col = region_index % PP_REGION_COLS;
        int region_row = region_index / PP_REGION_COLS;

        memset(&initial_state, 0, sizeof(initial_state));
        initial_state.region_index = region_index;
        initial_state.start_x = region_col * PP_REGION_SIDE;
        initial_state.start_y = region_row * PP_REGION_SIDE;
        initial_state.width = PP_REGION_SIDE;
        initial_state.height = PP_REGION_SIDE;
        initial_state.rng_state = pp_rng_next(&model->bootstrap_rng);
        initial_state.animals = NULL;
        initial_state.animal_count = 0;
        initial_state.animal_capacity = 0;
        initial_state.next_local_animal_serial = 1;
        initial_state.active_phase_kind = PP_MSG_PHASE_MOVE_PREDATORS;
        initial_state.phase_day = 0;
        initial_state.phase_step = 0;
        initial_state.pending_migration_responses = 0;
        initial_state.phase_done_sent = false;

        model->region_actors[region_index] = maf_spawn_actor_collective(runtime,
                                                                        pp_owner_rank_for_region(runtime, region_index),
                                                                        &PP_REGION_ACTOR_TYPE,
                                                                        &initial_state,
                                                                        sizeof(initial_state));
    }
}

static void pp_add_initial_animal(MpiActorRuntime *runtime,
                                  PPSpecies species,
                                  int x,
                                  int y) {
    PredatorPreyModel *model = pp_model(runtime);
    int region_index = pp_region_index_from_coords(x, y);
    MpiActorId region_actor_id = model->region_actors[region_index];
    MpiActor *region_actor = maf_get_local_actor(runtime, region_actor_id);

    if (!region_actor) {
        return;
    }

    PPRegionState *region = pp_region_state(region_actor);

    if (pp_region_cell_occupancy(region, x, y) >= PP_MAX_PER_CELL) {
        return;
    }

    pp_region_add_animal_record(region,
                                pp_region_next_animal_id(region),
                                species,
                                x,
                                y,
                                false);
}

static void pp_seed_initial_population(MpiActorRuntime *runtime) {
    PredatorPreyModel *model = pp_model(runtime);

    for (int i = 0; i < PP_INITIAL_PREY; i++) {
        int x = pp_rng_mod(&model->bootstrap_rng, PP_GRID_SIZE);
        int y = pp_rng_mod(&model->bootstrap_rng, PP_GRID_SIZE);
        pp_add_initial_animal(runtime, PP_SPECIES_PREY, x, y);
    }

    for (int i = 0; i < PP_INITIAL_PREDATORS; i++) {
        int x = pp_rng_mod(&model->bootstrap_rng, PP_GRID_SIZE);
        int y = pp_rng_mod(&model->bootstrap_rng, PP_GRID_SIZE);
        pp_add_initial_animal(runtime, PP_SPECIES_PREDATOR, x, y);
    }
}

static void pp_print_validation_summary(MpiActorRuntime *runtime, PredatorPreyModel *model) {
    const MpiActorValidationMetrics *framework_metrics = maf_validation_metrics(runtime);
    double local_model_values[17];
    double global_model_sum[17];
    double local_framework_values[7];
    double global_framework_sum[7];
    double local_run_wall_seconds;
    double global_run_wall_seconds;

    local_model_values[0] = model->total_step_seconds;
    local_model_values[1] = model->total_day_seconds;
    local_model_values[2] = model->move_decision_seconds;
    local_model_values[3] = model->occupancy_seconds;
    local_model_values[4] = model->rng_seconds;
    local_model_values[5] = model->allocation_seconds;
    local_model_values[6] = (double) model->completed_steps;
    local_model_values[7] = (double) model->completed_days;
    local_model_values[8] = (double) model->occupancy_calls;
    local_model_values[9] = (double) model->rng_calls;
    local_model_values[10] = (double) model->allocation_events;
    local_model_values[11] = (double) model->occupancy_violations;
    local_model_values[12] = (double) model->duplicate_id_violations;
    local_model_values[13] = (double) model->migration_requests;
    local_model_values[14] = (double) model->migration_accepted;
    local_model_values[15] = (double) model->migration_denied;
    local_model_values[16] = 0.0;
    local_run_wall_seconds = pp_now_seconds() - model->run_start_seconds;

    local_framework_values[0] = framework_metrics ? framework_metrics->run_wall_seconds : 0.0;
    local_framework_values[1] = framework_metrics ? framework_metrics->poll_incoming_seconds : 0.0;
    local_framework_values[2] = framework_metrics ? framework_metrics->reap_pending_sends_seconds : 0.0;
    local_framework_values[3] = framework_metrics ? framework_metrics->dispatch_seconds : 0.0;
    local_framework_values[4] = framework_metrics ? framework_metrics->idle_hook_seconds : 0.0;
    local_framework_values[5] = framework_metrics ? (double) framework_metrics->messages_dispatched : 0.0;
    local_framework_values[6] = framework_metrics
        ? (double) (framework_metrics->local_messages_sent + framework_metrics->remote_messages_sent)
        : 0.0;

    MPI_Reduce(local_model_values, global_model_sum, 17, MPI_DOUBLE, MPI_SUM, 0, runtime->comm);
    MPI_Reduce(local_framework_values, global_framework_sum, 7, MPI_DOUBLE, MPI_SUM, 0, runtime->comm);
    MPI_Reduce(&local_run_wall_seconds, &global_run_wall_seconds, 1, MPI_DOUBLE, MPI_MAX, 0, runtime->comm);

    if (maf_rank(runtime) != 0) {
        return;
    }

    fprintf(stderr, "VALIDATION_TIMING total_run_seconds=%.6f\n", global_run_wall_seconds);
    fprintf(stderr, "VALIDATION_TIMING total_step_seconds=%.6f completed_steps=%" PRIu64 " avg_step_seconds=%.6f\n",
            global_model_sum[0],
            (uint64_t) global_model_sum[6],
            global_model_sum[6] > 0.0 ? global_model_sum[0] / global_model_sum[6] : 0.0);
    fprintf(stderr, "VALIDATION_TIMING total_day_seconds=%.6f completed_days=%" PRIu64 " avg_day_seconds=%.6f\n",
            global_model_sum[1],
            (uint64_t) global_model_sum[7],
            global_model_sum[7] > 0.0 ? global_model_sum[1] / global_model_sum[7] : 0.0);
    fprintf(stderr, "VALIDATION_TIMING move_decision_seconds=%.6f\n", global_model_sum[2]);
    fprintf(stderr, "VALIDATION_TIMING occupancy_seconds=%.6f occupancy_calls=%" PRIu64 "\n",
            global_model_sum[3],
            (uint64_t) global_model_sum[8]);
    fprintf(stderr, "VALIDATION_TIMING rng_seconds=%.6f rng_calls=%" PRIu64 "\n",
            global_model_sum[4],
            (uint64_t) global_model_sum[9]);
    fprintf(stderr, "VALIDATION_TIMING allocation_seconds=%.6f allocation_events=%" PRIu64 "\n",
            global_model_sum[5],
            (uint64_t) global_model_sum[10]);
    fprintf(stderr, "VALIDATION_INVARIANTS occupancy_violations=%" PRIu64 " duplicate_id_violations=%" PRIu64 " migration_requests=%" PRIu64 " migration_accepted=%" PRIu64 " migration_denied=%" PRIu64 " migration_balance=%s\n",
            (uint64_t) global_model_sum[11],
            (uint64_t) global_model_sum[12],
            (uint64_t) global_model_sum[13],
            (uint64_t) global_model_sum[14],
            (uint64_t) global_model_sum[15],
            ((uint64_t) global_model_sum[13] == ((uint64_t) global_model_sum[14] + (uint64_t) global_model_sum[15])) ? "PASS" : "FAIL");
    fprintf(stderr, "VALIDATION_FRAMEWORK run_wall_seconds=%.6f poll_seconds=%.6f reap_seconds=%.6f dispatch_seconds=%.6f idle_seconds=%.6f messages_dispatched=%" PRIu64 " messages_sent=%" PRIu64 "\n",
            global_framework_sum[0],
            global_framework_sum[1],
            global_framework_sum[2],
            global_framework_sum[3],
            global_framework_sum[4],
            (uint64_t) global_framework_sum[5],
            (uint64_t) global_framework_sum[6]);
}

static void pp_initialise_model(MpiActorRuntime *runtime, PredatorPreyModel *model) {
    memset(model, 0, sizeof(*model));
    model->clock_actor = MAF_INVALID_ACTOR_ID;
    model->stats_actor = MAF_INVALID_ACTOR_ID;
    model->bootstrap_rng = pp_runtime_seed();
    model->validation_stride_days = pp_validation_stride_days();
    model->next_validation_day = 0;
    model->run_start_seconds = pp_now_seconds();
    model->current_day_start_seconds = model->run_start_seconds;
    model->current_step_start_seconds = model->run_start_seconds;
    pp_validation_model = model;

    maf_init(runtime, MPI_COMM_WORLD, model);

    pp_create_clock_actor(runtime);
    pp_create_stats_actor(runtime);
    pp_create_region_actors(runtime);
    pp_seed_initial_population(runtime);
}

int main(int argc, char **argv) {
    MpiActorRuntime runtime;
    PredatorPreyModel model;

    MPI_Init(&argc, &argv);

    pp_initialise_model(&runtime, &model);

    if (maf_rank(&runtime) == 0) {
        printf("Day,Prey,Predators\n");
        fflush(stdout);
    }

    maf_run(&runtime);
    pp_print_validation_summary(&runtime, &model);
    maf_destroy(&runtime);

    MPI_Finalize();
    return 0;
}
