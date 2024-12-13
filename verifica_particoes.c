#include <stdio.h>
#include "verifica_particoes.h"

void verifica_particoes(long long *Input, int n, long long *P, int np, long long *Output, int *Pos) {
    for (int j = 0; j < np; j++) {
        long long pMin = (j == 0) ? 0 : P[j - 1];
        long long pMax = P[j];
        int posStart = Pos[j];
        int posEnd = (j == np - 1) ? n : Pos[j + 1];

        // Verifica se os elementos estão dentro da faixa [pMin, pMax)
        for (int i = posStart; i < posEnd; i++) {
            if (Output[i] < pMin || Output[i] >= pMax) {
                printf("====> particionamento COM ERROS\n");
                printf("Erro no elemento Output[%d] = %lld na partição [%lld, %lld)\n", 
                       i, Output[i], pMin, pMax);
                return;
            }
        }
    }

    // Confirmação de que todos os elementos foram particionados corretamente
    printf("====> particionamento CORRETO\n");
}