#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include "chrono.h" // Presume-se que o cabeçalho esteja no seu projeto

#define RAND_MAX_CUSTOM 10

typedef struct {
    int size;
    int maxSize;
    long long *array;
} localOutput_t;

// Funções auxiliares
int compara(const void *a, const void *b) {
    long long valA = *(const long long *)a;
    long long valB = *(const long long *)b;
    if (valA < valB) return -1;
    if (valA > valB) return 1;
    return 0;
}

long long gera_aleatorio_ll() {
    return ((long long)rand() << 32) | rand();
}

void insereLocalOutput(localOutput_t *localOutput, long long num) {
    if (localOutput->size == localOutput->maxSize) {
        long long *temp = realloc(localOutput->array, sizeof(long long) * localOutput->maxSize * 2);
        if (!temp) return;
        localOutput->array = temp;
        localOutput->maxSize *= 2;
    }
    localOutput->array[localOutput->size] = num;
    localOutput->size++;
}

int upper_bound(long long *arr, int size, long long value) {
    int left = 0, right = size - 1;
    while (left <= right) {
        int mid = left + (right - left) / 2;
        if (arr[mid] <= value) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return left;
}

void multi_partition_mpi(long long *input, int n, long long *P, int np, long long *output, int *nO) {
    int processId;
    MPI_Comm_rank(MPI_COMM_WORLD, &processId);

    long long *partialResults = calloc(np * n, sizeof(long long));
    for (int i = 0; i < np * n; i++) {
        partialResults[i] = -1;
    }

    long long *rcvBuffer = calloc(np * n, sizeof(long long));
    int localPos[np];
    for (int i = 0; i < np; i++) {
        localPos[i] = 0;
    }

    for (int i = 0; i < n; i++) {
        int partitionIdx = upper_bound(P, np, input[i]);
        partialResults[partitionIdx * n + localPos[partitionIdx]] = input[i];
        localPos[partitionIdx]++;
    }

    MPI_Alltoall(partialResults, n, MPI_LONG_LONG, rcvBuffer, n, MPI_LONG_LONG, MPI_COMM_WORLD);

    int k = 0;
    for (int i = 0; i < np * n; i++) {
        if (rcvBuffer[i] != -1) {
            output[k] = rcvBuffer[i];
            k++;
        }
    }
    *nO = k;

    free(partialResults);
    free(rcvBuffer);
    MPI_Barrier(MPI_COMM_WORLD);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("Uso: %s <nTotalElements> <np>\n", argv[0]);
        return 1;
    }

    int nTotalElements = atoi(argv[1]);
    int np;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &np);
    int processId;
    MPI_Comm_rank(MPI_COMM_WORLD, &processId);

    int n = nTotalElements / np;
    srand(2024 * 100 + processId);

    long long *input = malloc(sizeof(long long) * n);
    long long *output = malloc(sizeof(long long) * np * n);
    long long *partitionArr = malloc(sizeof(long long) * np);

    for (int i = 0; i < n; i++) {
        input[i] = gera_aleatorio_ll();
    }

    if (processId == 0) {
        for (int i = 0; i < np - 1; i++) {
            partitionArr[i] = gera_aleatorio_ll();
        }
        qsort(partitionArr, np - 1, sizeof(long long), compara);
        partitionArr[np - 1] = LLONG_MAX;
    }

    MPI_Bcast(partitionArr, np, MPI_LONG_LONG, 0, MPI_COMM_WORLD);

    int nO;
    multi_partition_mpi(input, n, partitionArr, np, output, &nO);

    if (processId == 0) {
        printf("Resultado final:\n");
        for (int i = 0; i < nO; i++) {
            printf("%lld ", output[i]);
        }
        printf("\n");
    }

    free(input);
    free(output);
    free(partitionArr);
    MPI_Finalize();

    return 0;
}
