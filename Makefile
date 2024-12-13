# Variáveis de compilador e flags
CC = mpicc
CFLAGS = -Wall -O2

# Nome do executável
EXEC = main

# Fontes e objetos
SRCS = main.c verifica_particoes.c chrono.c
OBJS = $(SRCS:.c=.o)

# Regra principal (default)
all: $(EXEC)

# Regra para compilar o executável
$(EXEC): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# Regra genérica para compilar arquivos .c em .o
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Limpeza dos arquivos gerados
clean:
	rm -f $(OBJS) $(EXEC)
