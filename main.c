#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <time.h>

#include "mpi.h"
#include "chrono.h"
#include "verifica_particoes.h"

#define RAND_MAX_CUSTOM 10
#define MAX_THREADS 8
#define BATCH_SIZE 10
#define NTIMES 1

typedef struct {
    int size;
    int maxSize;
    long long *array;
} localOutput_t;

int *geraVetorPos(int n) {
    int *vetor = malloc(sizeof(int) * n);
    if (!vetor)
        return NULL;
    
    for (int i = 0; i < n; i++) {
        vetor[i] = 0;
    }    
    return vetor;
}

int compara(const void *a, const void *b) {
    long long valA = *(const long long *)a;
    long long valB = *(const long long *)b;
    if (valA < valB) return -1;
    if (valA > valB) return 1;
    return 0;
}

long long geraAleatorioLL() {
    int a = rand();  // Returns a pseudo-random integer
                //    between 0 and RAND_MAX.
    int b = rand();  // same as above
    long long v = (long long)a * 100 + b;
    return v;
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

void somaAcumulativa(int *Pos, int np) {
    long long soma = 0;

    for (int i=0; i <np; i++) {
        soma=Pos[i] + soma;
        Pos[i] = soma;
    }
}

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

void verifica_particoes2(long long *Input, int n, long long *P, int np, 
                        long long *Output, int *nO) {
    int processId;
    MPI_Comm_rank(MPI_COMM_WORLD, &processId);

    int isCorrect = 1;

    for (int i = 0; i < *nO; i++) {
        if (Output[i] < (processId == 0 ? 0 : P[processId-1])
            || Output[i] >= P[processId]) {
            isCorrect = 0;
            break;
        }
    }

    // Exibir resultado final
    if (isCorrect) {
        printf("===> particionamento CORRETO\n");
    } else {
        printf("===> particionamento COM ERROS no processo %d\n", processId);
    }
}

int procuraVetor2(long long *P, int tamanho, long long x) {
    int inicio = 0, fim = tamanho - 1, meio;

    while (inicio <= fim) {
        meio = inicio + (fim - inicio) / 2;

        if (x < P[meio]) {
            fim = meio - 1;
        } else {
            inicio = meio + 1;
        }
    }

    // Retorna o índice da faixa correspondente
    return inicio;
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

// void multi_partition( long long *Input, int n, long long *P, int np, long long *Output, int *nO ) {
//     localOutput_t *localOutputs = malloc(sizeof(localOutput_t)*np);

//     MPI_Comm_rank(MPI_COMM_WORLD, &processId);

//     int processId;
//     int Pos[np];
//     int size = n / np;

//     for(int i=0; i<np; i++) {
//         localOutputs[i].size = 0;
//         localOutputs[i].maxSize = size;
//         localOutputs[i].array = malloc(sizeof(long long)*size); 
//     }
    
//     int retorno = 0;
//     for(int i=0; i<n; i++) {
//         retorno = procuraVetor(P, np, Input[i], localOutputs);
//         Pos[retorno+1]++;
//         insereLocalOutput(&localOutputs[retorno], Input[i], np);
//     }

//     int countOutput = 0;
//     for(int i=0; i<np; i++) {
//         for(int j=0; j<localOutputs[i].size; j++) {
//             Output[countOutput++] = localOutputs[i].array[j];
//         }
//     }

//     for(int i=0; i<np; i++)
//         free(localOutputs[i].array);
//     free(localOutputs); 

//     *nO = k;

//     MPI_Barrier(MPI_COMM_WORLD);

// }
void multi_partition(long long *Input, int n, long long *P, int np, long long *Output, int *nO) {
    int processId, nProcesses;
    MPI_Comm_rank(MPI_COMM_WORLD, &processId);
    MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);

    // 1. Inicialização local de `localOutputs`
    int nLocal = n / nProcesses; // Número de elementos por processo

    localOutput_t *localOutputs = malloc(sizeof(localOutput_t) * np);
    for (int i = 0; i < np; i++) {
        localOutputs[i].size = 0;
        localOutputs[i].maxSize = nLocal;
        localOutputs[i].array = malloc(sizeof(long long) * nLocal);
    }

    // 2. Particionamento local
    int *sendCounts = calloc(np, sizeof(int)); // Contagem de elementos para cada faixa
    for (int i = 0; i < nLocal; i++) {
        int faixa = procuraVetor2(P, np, Input[i]); // Faixa do elemento
        insereLocalOutput(&localOutputs[faixa], Input[i], np);
        sendCounts[faixa]++;
    }

    // Preparar dados para MPI_Alltoallv
    long long *sendBuffer = malloc(sizeof(long long) * nLocal);
    int *sendDispls = malloc(sizeof(int) * np); // Deslocamentos
    int offset = 0;
    for (int i = 0; i < np; i++) {
        sendDispls[i] = offset;
        memcpy(sendBuffer + offset, localOutputs[i].array, sizeof(long long) * localOutputs[i].size);
        offset += localOutputs[i].size;
    }

    // 3. Redistribuição com MPI_Alltoallv
    int *recvCounts = calloc(np, sizeof(int));
    int *recvDispls = calloc(np, sizeof(int));

    MPI_Alltoall(sendCounts, 1, MPI_INT, recvCounts, 1, MPI_INT, MPI_COMM_WORLD);

    int totalRecv = 0;
    for (int i = 0; i < np; i++) {
        recvDispls[i] = totalRecv;
        totalRecv += recvCounts[i];
    }

    long long *recvBuffer = malloc(sizeof(long long) * totalRecv);

    MPI_Alltoallv(sendBuffer, sendCounts, sendDispls, MPI_LONG_LONG, recvBuffer, recvCounts, recvDispls, MPI_LONG_LONG, MPI_COMM_WORLD);

    // 4. Concatenar os dados recebidos no vetor Output
    memcpy(Output, recvBuffer, sizeof(long long) * totalRecv);
    *nO = totalRecv;

    // Liberação de memória
    for (int i = 0; i < np; i++) {
        free(localOutputs[i].array);
    }

    free(localOutputs);
    free(sendCounts);
    free(sendDispls);
    free(recvCounts);
    free(recvDispls);
    free(sendBuffer);
    free(recvBuffer);

    MPI_Barrier(MPI_COMM_WORLD);
}

int main(int argc, char *argv[]) {
    // Recebe o número de threads pelo argv
    // srand(time(NULL));
    if (argc != 2) {
        printf("Uso: %s <n> <np>\n", argv[0]);
        return 1;
    }
    // agora o vetor comeca vazio e dentro de cada processo ele é gerado
    // long long *P = geraVetor(np, 1);
    // int *Pos = geraVetorPos(np);

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &np);
    MPI_Comm_rank(MPI_COMM_WORLD, &processId);

    
    int nTotalElements = atoi(argv[1]);
    int np = atoi(argv[2]);

    // nO é o número de elementos no vetor de saída
    int processId, nO;

    int nLocal = nTotalElements / np;

    long long *Input = malloc(sizeof(long long) * nTotalElements);
    long long *Output = malloc(sizeof(long long) * np * nLocal);

    srand(2024 * 100 + processId);

    /// Medição do tempo de execução
    printf("--- Executando o multi_partition ---\n");
    printf("n = %d\nnp = %d\n", nTotalElements, np);

    //gera aleatorio input 
    for (int i = 0; i < nLocal; i++) {
         Input[i] = geraAleatorioLL();
    }

    long long *P = malloc(sizeof(long long) * (np));
    
    // o processo 0 gera o vetor P e envia para os outros processos
    if (processId == 0) {
        for (int i = 0; i < np-1; i++) {
            P[i] = geraAleatorioLL();
        }
        qsort(P, np-1, sizeof(long long), compara);
        // professor pediu omaior tamanho de long long na ultima posicao
        P[np-1] = LLONG_MAX;
    }

    // MPI manda por broadcast o vetor P para todos os processos
    MPI_Bcast(partitionArr, np, MPI_LONG_LONG, 0, MPI_COMM_WORLD);

    chronometer_t time;
    chrono_reset(&time);
    chrono_start(&time);

    for (int i=0; i< NTIMES; i++){
        multi_partition(Input, nLocal, P, np, Output, &nO);
    }

    // o MPI faz isso MPI_Alltoall
    //somaAcumulativa(Pos, np);

    chrono_stop(&time);

    verifica_particoes2(Input, nLocal, P, np, Output, &nO);
    //verifica_particoes(Input, nTotalElements, P, np, Output, Pos);
    double total_time_in_nanoseconds = (double) chrono_gettotal(&time);
    double total_time_in_seconds = total_time_in_nanoseconds / (1000 * 1000 * 1000);
    printf("total_time_in_seconds: %lf s\n", total_time_in_seconds);

    // printVetor(Input, n);
    // printVetor(Output, n);
    // printVetor(P, np);
    // printVetorPos(Pos, np);

    free(Input);
    free(Output);
    free(P);
}