#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>

// Simulation parameters
#ifndef GRID_SIZE
#define GRID_SIZE 40
#endif

#ifndef MAX_PER_CELL
#define MAX_PER_CELL 100
#endif

#ifndef INITIAL_PREY
#define INITIAL_PREY 1600
#endif

#ifndef INITIAL_PREDATORS
#define INITIAL_PREDATORS 400
#endif

#ifndef STEPS_PER_DAY
#define STEPS_PER_DAY 40
#endif

#ifndef DAYS
#define DAYS 20
#endif

#ifndef PP_SEED
#define PP_SEED 123456u
#endif

// Lotka–Volterra parameters
#define ALPHA 0.06     // prey birth rate
#define BETA  0.01     // predation rate
#define DELTA 0.1      // predator reproduction per prey eaten
#define GAMMA 0.04     // predator death rate


typedef enum { PREY, PREDATOR } Species;

typedef struct Animal {
    Species species;
    int x, y;
    struct Animal *next;
} Animal;

Animal *predators = NULL;
Animal *preys = NULL;

int preyCount = 0;
int predatorCount = 0;
int cellCount[GRID_SIZE][GRID_SIZE] = {0};
bool allExtinct = false;

double randDouble() {

    return rand() / (double)RAND_MAX;
}

void addAnimal(Species s, int x, int y) {

    if (cellCount[x][y] >= MAX_PER_CELL) return;
    Animal *a = malloc(sizeof(Animal));
    a->species = s;
    a->x = x;
    a->y = y;

    if (s == PREY) {
        preyCount++;
        a->next = preys;
        preys = a;
    }
    else {
        predatorCount++;
        a->next = predators;
        predators = a;
    }
    cellCount[x][y]++;
}

void moveAnimal(Animal *a) {

    int dx = (rand() % 3) - 1;
    int dy = (rand() % 3) - 1;

    int newX = (a->x + dx + GRID_SIZE) % GRID_SIZE;
    int newY = (a->y + dy + GRID_SIZE) % GRID_SIZE;

    if (cellCount[newX][newY] < MAX_PER_CELL) {
        cellCount[a->x][a->y]--;
        a->x = newX;
        a->y = newY;
        cellCount[newX][newY]++;
    }
}

void reproducePrey(Animal *a) {
    if (randDouble() < ALPHA) {
        addAnimal(PREY, a->x, a->y);
    }
}

void resolvePredation() {

    Animal **predPtr = &predators;

    while (*predPtr) {

        Animal *pred = *predPtr;
        Animal **preyPtr = &preys;

        while (*preyPtr) {

            Animal *prey = *preyPtr;

            if (prey->x == pred->x &&
                prey->y == pred->y) {

                if (randDouble() < BETA) {
                    cellCount[prey->x][prey->y]--;
                    *preyPtr = prey->next;
                    free(prey);
                    preyCount--;

                    if (randDouble() < DELTA) {
                        addAnimal(PREDATOR, pred->x, pred->y);
                    }

                    break;
                }
            }

            preyPtr = &((*preyPtr)->next);
        }
        predPtr = &((*predPtr)->next);
    }
}

void applyPredatorDeath() {

    Animal **ptr = &predators;

    while (*ptr) {

        Animal *a = *ptr;

        if (randDouble() < GAMMA) {
            cellCount[a->x][a->y]--;

            *ptr = a->next;
            free(a);
            predatorCount--;
            continue;
        }

        ptr = &((*ptr)->next);
    }
}

void simulateStep() {

    // Move all animals
    Animal *a = predators;
    while (a) {
        moveAnimal(a);
        a = a->next;
    }

    a = preys;
    while (a) {
        moveAnimal(a);
        a = a->next;
    }

    // Predation + predator reproduction
    resolvePredation();

    // Predator natural death
    applyPredatorDeath();

    // Prey reproduction
    a = preys;
    while (a) {
        reproducePrey(a);
        a = a->next;
    }
}

void initialise() {

    for (int i = 0; i < INITIAL_PREY; i++) {
        addAnimal(PREY,
                  rand() % GRID_SIZE,
                  rand() % GRID_SIZE);
    }

    for (int i = 0; i < INITIAL_PREDATORS; i++) {
        addAnimal(PREDATOR,
                  rand() % GRID_SIZE,
                  rand() % GRID_SIZE);
    }
}

void freeAll() {

    Animal *a = preys;
    while (a) {
        Animal *next = a->next;
        free(a);
        a = next;
    }
    a = predators;
    while (a) {
        Animal *next = a->next;
        free(a);
        a = next;
    }
}

int main() {

    srand((unsigned int) PP_SEED);

    initialise();

    printf("Day,Prey,Predators\n");

    for (int day = 0; day < DAYS; day++) {
        for (int step = 0; step < STEPS_PER_DAY; step++) {

            simulateStep();

            if (preyCount == 0 && predatorCount == 0) {
                printf("All animals extinct.\n");
                allExtinct = true;
                break;
            }
        }

        if (allExtinct) {
            break;
        }
        printf("%d,%d,%d\n", day, preyCount, predatorCount);
    }

    freeAll();
    return 0;
}
