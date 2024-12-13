#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <time.h>
#include "verifica_particoes.h"
#include <pthread.h>
#define RAND_MAX_CUSTOM 10
#define MAX_THREADS 8
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

pthread_mutex_t mutexQueue;
pthread_mutex_t mutexLocalOutput;

Task_t taskQueue[8000000];
int taskCount = 0;



// FUNÇÕES GERAIS
void printVetor(long long *Output, int n) {
    for(int i=0; i<n; i++) {
        printf("%lld ", Output[i]);
    }
    printf("\n");
}

void printVetorPos(int *Pos, int n) {
    for(int i=0; i<n; i++) {
        printf("%d ", Pos[i]);
    }
    printf("\n");
}

void insereLocalOutput(localOutput_t *localOutput, long long num, int np) {
    long long *temp;
    if(localOutput->size == localOutput->maxSize) {
        temp = realloc(localOutput->array, sizeof(long long) * localOutput->maxSize * 2);
        if(!temp)
            return;
        localOutput->array = temp;
        localOutput->maxSize = localOutput->maxSize*2;
    }
    localOutput->array[localOutput->size] = num;
    localOutput->size++;
}

void somaAcumulativa(int *Pos, int np) {
    long long soma = 0;

    for (int i=0; i <np; i++) {
        soma=Pos[i] + soma;
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
        vetor[n-1] = LLONG_MAX;
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



// FUNÇÕES NÃO PARALELIZADAS
int procuraVetor(long long *P, int tamanho, long long x, localOutput_t *localOutputs) {
    int inicio = 0, fim = tamanho - 1, meio;

    while (inicio <= fim) {
        meio = inicio + (fim - inicio) / 2;

        if (P[meio] == x) {
            return meio+1;
        } else if (P[meio] < x) {
            inicio = meio + 1;
        } else {
            fim = meio - 1;
        }
    }

    return inicio;
}

void multi_partition(int n, int np, long long *P, long long *Input, int *Pos, long long *Output) {
    localOutput_t *localOutputs = malloc(sizeof(localOutput_t)*np);
    int size = n / np;
    for(int i=0; i<np; i++) {
        localOutputs[i].size = 0;
        localOutputs[i].maxSize = size;
        localOutputs[i].array = malloc(sizeof(long long)*size); 
    }
    
    int retorno = 0;
    for(int i=0; i<n; i++) {
        retorno = procuraVetor(P, np, Input[i], localOutputs);
        Pos[retorno+1]++;
        insereLocalOutput(&localOutputs[retorno], Input[i], np);
    }

    int countOutput = 0;
    for(int i=0; i<np; i++) {
        for(int j=0; j<localOutputs[i].size; j++) {
            Output[countOutput++] = localOutputs[i].array[j];
        }
    }

    for(int i=0; i<np; i++)
        free(localOutputs[i].array);
    free(localOutputs); 

}



// FUNÇÕES PARALELIZADAS
void submitTask(Task_t task) {
    pthread_mutex_lock(&mutexQueue);
    taskQueue[taskCount] = task;
    taskCount++;
    pthread_mutex_unlock(&mutexQueue);
}

void executeTask(Task_t *task) {
    long long *P = task->P;
    int np = task->np;
    long long num = task->num;
    int inicio = 0, fim = np - 1, meio;
    
    while (inicio <= fim) {
        meio = inicio + (fim - inicio) / 2;

        if (P[meio] == num) {
            task->numPos = meio+1;
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

void *startThread() {
    Task_t batchTasks[BATCH_SIZE];
    int batchCount;

    while (1) {
        // Pegar um lote de tarefas da fila
        batchCount = 0;
        pthread_mutex_lock(&mutexQueue);
        while (taskCount > 0 && batchCount < BATCH_SIZE) {
            batchTasks[batchCount++] = taskQueue[--taskCount];
        }
        pthread_mutex_unlock(&mutexQueue);

        // Se não houver mais tarefas, sair
        if (batchCount == 0) {
            pthread_mutex_lock(&mutexQueue);
            if (taskCount == 0) {
                pthread_mutex_unlock(&mutexQueue);
                return NULL;
            }
            pthread_mutex_unlock(&mutexQueue);
        }

        // Executar o lote de tarefas
        for (int i = 0; i < batchCount; i++) {
            executeTask(&batchTasks[i]);

            // Atualizar os resultados
            pthread_mutex_lock(&mutexLocalOutput);
            batchTasks[i].Pos[batchTasks[i].numPos + 1]++;
            insereLocalOutput(&batchTasks[i].localOutputs[batchTasks[i].numPos], 
                              batchTasks[i].num, 
                              batchTasks[i].np);
            pthread_mutex_unlock(&mutexLocalOutput);
        }
    }
}


void multi_partition_mpi(int n, int np, long long *P, long long *Input, int *Pos, long long *Output) {
    localOutput_t *localOutputs = malloc(sizeof(localOutput_t)*np);
    int size = n / np;
    for(int i=0; i<np; i++) {
        localOutputs[i].size = 0;
        localOutputs[i].maxSize = size;
        localOutputs[i].array = malloc(sizeof(long long)*size); 
    }
    // ---------------- POOL DE THREADS ----------------
    for(int i=0; i<n; i++) {
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
        submitTask(t);
    }
    printf("adicionou todas as tasks!\n");

    pthread_t th[MAX_THREADS];
    for(int i=0; i<MAX_THREADS; i++) {
        if(pthread_create(&th[i], NULL, &startThread, NULL) != 0)
            perror("Falhou em criar a thread!\n");
    }
    for(int i=0; i<MAX_THREADS; i++) {
        if(pthread_join(th[i], NULL) != 0)
            perror("Falhou em entrar na thread!\n");
    }
    // -------------------------------------------------
    
    // Aguarda terminar todas as tasks
    while(taskCount > 0) {
        continue;
    }
    printf("todos terminaram!");

    int countOutput = 0;
    for(int i=0; i<np; i++) {
        for(int j=0; j<localOutputs[i].size; j++) {
            Output[countOutput++] = localOutputs[i].array[j];
        }
    }

    // for(int i=0; i<np; i++)
    //     free(localOutputs[i].array);
    // free(localOutputs); 

}


int main(int argc, char *argv[]) {
    // Recebe o número de threads pelo argv
    // srand(time(NULL));
    if (argc != 3) {
        printf("Uso: %s <n> <np>\n", argv[0]);
        return 1;
    }
    int n = atoi(argv[1]);
    int np = atoi(argv[2]);
    printf("n = %d, np = %d\n", n, np);

    // Cria estruturas
    long long *Input = geraVetor(n, 0);
    long long *Output = geraVetor(n, 0);
    long long *P = geraVetor(np, 1);
    int *Pos = geraVetorPos(np);


    pthread_mutex_init(&mutexQueue, NULL);
    pthread_mutex_init(&mutexLocalOutput, NULL);


    /// Medição do tempo de execução
    struct timespec start, end;
    printf("--- Executando o multi_partition ---\n");
    printf("n = %d\nnp = %d\n", n, np);
    clock_gettime(CLOCK_REALTIME, &start);

    multi_partition_mpi(n, np, P, Input, Pos, Output);
    somaAcumulativa(Pos, np);
    verifica_particoes(Input, n, P, np, Output, Pos);

    clock_gettime(CLOCK_REALTIME, &end);
    double tempo = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
    printf("total_time_in_seconds: %lf s\n", tempo);

    // printVetor(Input, n);
    // printVetor(Output, n);
    // printVetor(P, np);
    // printVetorPos(Pos, np);

    pthread_mutex_destroy(&mutexLocalOutput);
    pthread_mutex_destroy(&mutexQueue);
    free(Input);
    free(Output);
    free(Pos);
    free(P);
}