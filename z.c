#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <time.h>
#include "verifica_particoes.h"
#define RAND_MAX_CUSTOM 10
#define MAX_THREADS 4

typedef struct {
    int size;
    int maxSize;
    long long *array;
} localOutput_t;

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

void somaAcumulativa(int *Pos, int np) {
    long long soma = 0;

    for (int i=0; i <np; i++) {
        soma=Pos[i] + soma;
        Pos[i] = soma;
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

// Função para gerar o vetor
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

// Função para gerar o vetor Pos
int *geraVetorPos(int n) {
    int *vetor = malloc(sizeof(int) * n);
    if (!vetor)
        return NULL;
    
    for (int i = 0; i < n; i++) {
        vetor[i] = 0;
    }    
    return vetor;
}


int main(int argc, char *argv[]) {
    // Recebe o número de threads pelo argv
    srand(time(NULL));
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
    multi_partition_mpi(n, np, P, Input, Pos, Output);
    somaAcumulativa(Pos, np);
    verifica_particoes(Input, n, P, np, Output, Pos);

    // printVetor(Input, n);
    // printVetor(Output, n);
    // printVetor(P, np);
    // printVetorPos(Pos, np);

}