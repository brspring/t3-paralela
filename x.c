#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <mpi.h>

// Funções para paralelismo com MPI

void executeTask(Task_t *task) {
    long long *P = task->P;
    int np = task->np;
    long long num = task->num;
    int inicio = 0, fim = np - 1, meio;
    
    while (inicio <= fim) {
        meio = inicio + (fim - inicio) / 2;

        if (P[meio] == num) {
            task->numPos = meio + 1;
            return;
        } else if (P[meio] < num) {
            inicio = meio + 1;
        } else {
            fim = meio - 1;
        }
    }

    task->numPos = inicio;
    return;
}

// Função MPI para distribuir as tarefas e coletar resultados
void multi_partition_mpi(int n, int np, long long *P, long long *Input, int *Pos, long long *Output) {
    int rank, size;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Número de tarefas por processo
    int tasks_per_process = n / size;
    int start = rank * tasks_per_process;
    int end = (rank == size - 1) ? n : start + tasks_per_process;

    // Cada processo executa a sua parte
    localOutput_t *localOutputs = malloc(sizeof(localOutput_t) * np);
    for (int i = 0; i < np; i++) {
        localOutputs[i].size = 0;
        localOutputs[i].maxSize = tasks_per_process;
        localOutputs[i].array = malloc(sizeof(long long) * tasks_per_process);
    }

    // Adiciona tarefas específicas para o processo
    for (int i = start; i < end; i++) {
        Task_t t = {
            .Input = Input,
            .Output = Output,
            .P = P,
            .Pos = Pos,
            .n = n,
            .np = np,
            .localOutputs = localOutputs,
            .num = Input[i],
            .numPos = 0
        };
        executeTask(&t);
        // Comunicação entre os processos para coletar resultados
        MPI_Gather(t.localOutputs, sizeof(localOutput_t) * np, MPI_BYTE, Output, sizeof(localOutput_t) * np, MPI_BYTE, 0, MPI_COMM_WORLD);
    }

    // Finaliza o MPI
    MPI_Finalize();
}

int main(int argc, char *argv[]) {
    // Recebe o número de threads pelo argv
    if (argc != 3) {
        printf("Uso: %s <n> <np>\n", argv[0]);
        return 1;
    }
    int n = atoi(argv[1]);
    int np = atoi(argv[2]);
    printf("n = %d, np = %d\n", n, np);

    // Cria as estruturas
    long long *Input = geraVetor(n, 0);
    long long *Output = geraVetor(n, 0);
    long long *P = geraVetor(np, 1);
    int *Pos = geraVetorPos(np);

    // Inicia o MPI
    MPI_Init(NULL, NULL);
    
    struct timespec start, end;
    printf("--- Executando o multi_partition ---\n");
    printf("n = %d\nnp = %d\n", n, np);
    clock_gettime(CLOCK_REALTIME, &start);

    // Chama a função MPI para realizar a partição e processamento
    multi_partition_mpi(n, np, P, Input, Pos, Output);

    // Finaliza a execução
    somaAcumulativa(Pos, np);
    verifica_particoes(Input, n, P, np, Output, Pos);

    clock_gettime(CLOCK_REALTIME, &end);
    double tempo = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf("total_time_in_seconds: %lf s\n", tempo);

    // Finaliza o MPI
    MPI_Finalize();

    // Libera memória
    free(Input);
    free(Output);
    free(Pos);
    free(P);
    return 0;
}
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <time.h>
#include "verifica_particoes.h"
#include <mpi.h>

#define RAND_MAX_CUSTOM 10
#define BATCH_SIZE 10

typedef struct {
    int size;
    int maxSize;
    long long *array;
} localOutput_t;

typedef struct {
    long long *Input;
    long long *Output;
    long long *P;
    int *Pos;
    int n;
    int np;
    localOutput_t *localOutputs;
    long long num;
    int numPos;
} Task_t;

// Funções gerais
void printVetor(long long *Output, int n) {
    for (int i = 0; i < n; i++) {
        printf("%lld ", Output[i]);
    }
    printf("\n");
}

void printVetorPos(int *Pos, int n) {
    for (int i = 0; i < n; i++) {
        printf("%d ", Pos[i]);
    }
    printf("\n");
}

void insereLocalOutput(localOutput_t *localOutput, long long num, int np) {
    long long *temp;
    if (localOutput->size == localOutput->maxSize) {
        temp = realloc(localOutput->array, sizeof(long long) * localOutput->maxSize * 2);
        if (!temp)
            return;
        localOutput->array = temp;
        localOutput->maxSize = localOutput->maxSize * 2;
    }
    localOutput->array[localOutput->size] = num;
    localOutput->size++;
}

void somaAcumulativa(int *Pos, int np) {
    long long soma = 0;

    for (int i = 0; i < np; i++) {
        soma = Pos[i] + soma;
        Pos[i] = soma;
    }
}

long long geraAleatorioLL() {
    long long a = rand();
    long long b = rand();
    long long v = (long long)a * 100 + b;
    return v;
}

int compara(const void *a, const void *b) {
    long long valA = *(const long long *)a;
    long long valB = *(const long long *)b;
    if (valA < valB) return -1;
    if (valA > valB) return 1;
    return 0;
}

long long *geraVetor(int n, int ordena) {
    long long *vetor = malloc(sizeof(long long) * n);
    if (!vetor)
        return NULL;
    
    for (long long i = 0; i < n; i++) {
        vetor[i] = geraAleatorioLL(); // Supondo que essa função existe
    }
    
    if (ordena) {
        qsort(vetor, n, sizeof(long long), compara);
        vetor[n - 1] = LLONG_MAX;
    }
    
    return vetor;
}

int *geraVetorPos(int n) {
    int *vetor = malloc(sizeof(int) * n);
    if (!vetor)
        return NULL;
    
    for (int i = 0; i < n; i++) {
        vetor[i] = 0;
    }    
    return vetor;
}


// FUNÇÕES PARALELIZADAS COM MPI
void executeTask(Task_t *task) {
    long long *P = task->P;
    int np = task->np;
    long long num = task->num;
    int inicio = 0, fim = np - 1, meio;
    
    while (inicio <= fim) {
        meio = inicio + (fim - inicio) / 2;

        if (P[meio] == num) {
            task->numPos = meio + 1;
            return;
        } else if (P[meio] < num) {
            inicio = meio + 1;
        } else {
            fim = meio - 1;
        }
    }

    task->numPos = inicio;
}

void multi_partition_mpi(int n, int np, long long *P, long long *Input, int *Pos, long long *Output) {
    localOutput_t *localOutputs = malloc(sizeof(localOutput_t) * np);
    int size = n / np;

    for (int i = 0; i < np; i++) {
        localOutputs[i].size = 0;
        localOutputs[i].maxSize = size;
        localOutputs[i].array = malloc(sizeof(long long) * size);
    }

    // Dividir as tarefas entre os processos MPI
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int chunk_size = n / np;
    int start_index = rank * chunk_size;
    int end_index = (rank == np - 1) ? n : (rank + 1) * chunk_size;

    // Cada processo trabalha em uma parte do vetor
    for (int i = start_index; i < end_index; i++) {
        Task_t t = {
            .Input = Input,
            .Output = Output,
            .P = P,
            .Pos = Pos,
            .n = n,
            .np = np,
            .localOutputs = localOutputs,
            .num = Input[i],
            .numPos = 0
        };
        executeTask(&t);
        insereLocalOutput(&localOutputs[rank], t.num, np);
    }

    // Agora, todos os processos precisam coletar os resultados de todos os outros
    MPI_Gather(localOutputs[rank].array, localOutputs[rank].size, MPI_LONG_LONG,
               Output, chunk_size, MPI_LONG_LONG, 0, MPI_COMM_WORLD);

    // Processo 0 faz a consolidação dos resultados
    if (rank == 0) {
        int countOutput = 0;
        for (int i = 0; i < np; i++) {
            for (int j = 0; j < localOutputs[i].size; j++) {
                Output[countOutput++] = localOutputs[i].array[j];
            }
        }
    }

    // Liberando memória
    for (int i = 0; i < np; i++) {
        free(localOutputs[i].array);
    }
    free(localOutputs);
}

int main(int argc, char *argv[]) {
    // Inicialização do MPI
    MPI_Init(&argc, &argv);
    
    if (argc != 3) {
        printf("Uso: %s <n> <np>\n", argv[0]);
        return 1;
    }

    int n = atoi(argv[1]);
    int np = atoi(argv[2]);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    printf("n = %d, np = %d, rank = %d\n", n, np, rank);

    // Cria estruturas
    long long *Input = NULL;
    long long *Output = NULL;
    long long *P = NULL;
    int *Pos = NULL;

    if (rank == 0) {
        Input = geraVetor(n, 0);
        Output = geraVetor(n, 0);
        P = geraVetor(np, 1);
        Pos = geraVetorPos(np);
    }

    // Divisão de dados
    MPI_Bcast(P, np, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
    MPI_Bcast(Pos, np, MPI_INT, 0, MPI_COMM_WORLD);
    
    // Medição do tempo de execução
    struct timespec start, end;
    if (rank == 0) {
        printf("--- Executando o multi_partition ---\n");
        clock_gettime(CLOCK_REALTIME, &start);
    }

    multi_partition_mpi(n, np, P, Input, Pos, Output);

    if (rank == 0) {
        somaAcumulativa(Pos, np);
        verifica_particoes(Input, n, P, np, Output, Pos);

        clock_gettime(CLOCK_REALTIME, &end);
        double tempo = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
        printf("total_time_in_seconds: %lf s\n", tempo);
    }

    // Libera memória
    if (rank == 0) {
        free(Input);
        free(Output);
        free(Pos);
        free(P);
    }

    MPI_Finalize();
    return 0;
}
