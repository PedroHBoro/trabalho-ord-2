# Hashing Extensível em Memória Secundária

## O Problema do Hashing Estático

Para entender o hashing extensível, primeiro precisamos lembrar das limitações do hashing tradicional (estático):

1.  **Tamanho Fixo:** A tabela de hash (e o arquivo que a representa) tem um tamanho pré-definido. Se você escolher um tamanho muito grande, desperdiçará espaço. Se escolher um muito pequeno, o arquivo encherá rapidamente.
2.  **Colisões e Desempenho:** Conforme mais dados são inseridos, a quantidade de colisões (chaves diferentes que geram o mesmo endereço) aumenta. Isso degrada o desempenho, pois o sistema precisa de mais acessos a disco para encontrar ou inserir um registro.
3.  **Reorganização Custa Caro:** Se o arquivo ficar muito cheio, a única solução é criar um novo arquivo, maior, e realocar todos os registros ("re-hashing"). Essa é uma operação extremamente custosa e demorada.

O **hashing extensível** foi criado para resolver exatamente esses problemas, permitindo que o espaço de endereçamento cresça e encolha dinamicamente.

---

## Como Funciona o Hashing Extensível

O hashing extensível combina duas estruturas principais para alcançar sua flexibilidade:

1.  **Buckets (Baldes):** São blocos de tamanho fixo armazenados no arquivo de dados em memória secundária. Cada bucket pode armazenar um ou mais registros. É a unidade de transferência entre o disco e a RAM.
2.  **Diretório (Directory):** É uma estrutura de dados auxiliar, como um array, que armazena "ponteiros" (na prática, o endereço ou RRN - Número Relativo de Registro) para os buckets no arquivo de dados. O diretório é pequeno o suficiente para ser mantido em memória principal (RAM), o que o torna muito rápido de acessar.

A "mágica" do hashing extensível está em como ele usa o diretório para gerenciar os buckets de forma dinâmica.

### Componentes Principais

*   **Função de Hash:** Gera um valor numérico (geralmente tratado como uma sequência de bits) para cada chave. Diferente do hashing estático, não usamos o operador `módulo` sobre o tamanho total do arquivo. Em vez disso, olhamos para os bits iniciais do resultado do hash.
*   **Profundidade Global (pG):** Um valor associado ao **diretório**. Ele indica quantos bits iniciais do hash devem ser usados para encontrar uma entrada no diretório. O tamanho do diretório é sempre **2^pG**.
*   **Profundidade Local (pL):** Um valor associado a cada **bucket**. Ele indica quantos bits de prefixo são comuns a todas as chaves armazenadas *naquele bucket específico*.

A regra fundamental é: **pL ≤ pG**. Várias entradas no diretório podem apontar para o mesmo bucket. Isso acontece quando a profundidade local de um bucket é menor que a profundidade global.

### Como Selecionar os 'x' Primeiros Bits com Operações Bitwise?

A operação central para encontrar o índice no diretório é "considerar os 'x' primeiros bits do hash". Em Python, a maneira mais eficiente de fazer isso é com **operações bitwise (bit a bit)**, especificamente o operador **AND (`&`)** com uma **máscara**.

A máscara é um número que tem `1`s nas posições dos bits que queremos manter e `0`s nas outras. Para criar uma máscara que seleciona os `x` bits da direita (os menos significativos), usamos a expressão `(1 << x) - 1`.

-   `1 << x`: Desloca o bit `1` para a esquerda `x` vezes. Por exemplo, `1 << 4` resulta em `10000` em binário (16).
-   `- 1`: Subtrair 1 desse número resulta em um número com `x` bits `1`. Por exemplo, `16 - 1` é `15`, que é `01111` em binário.

#### Exemplo Prático:

Vamos supor que nossa profundidade global (`pG`) é **4**, e o hash de uma chave é **179** (`10110011` em binário).

```python
# Valor de hash de exemplo
hash_valor = 179 # Binário: 10110011

# Queremos os 4 bits menos significativos (profundidade_global = 4)
profundidade_global = 4

# 1. Criar a máscara
# (1 << 4) se torna 16 (binário: 10000)
# 16 - 1 se torna 15 (binário: 01111)
mascara = (1 << profundidade_global) - 1

# 2. Aplicar a máscara com o operador AND para obter o índice
indice = hash_valor & mascara

# --- Verificação ---
print(f"Valor do Hash: {hash_valor}  (Binário: {bin(hash_valor)})")
print(f"Máscara:       {mascara}   (Binário: {bin(mascara)})")
# A operação bitwise:
#   10110011  (179)
# & 00001111  (15)
# ----------
#   00000011  (3)
print(f"Índice Resultante: {indice}    (Binário: {bin(indice)})")
# O resultado é 3. Esse será o índice no diretório.
```
Essa operação é exatamente o que a função `_get_indice_diretorio` faz no exemplo de código a seguir.

### Exemplo de Estruturas em Python

Vamos modelar as estruturas básicas em Python para ilustrar os conceitos.

```python
# Uma função de hash simples para demonstração
def hash_chave(chave: int) -> int:
    """Retorna um valor de hash para uma chave inteira."""
    return chave

class Bucket:
    """Representa um bucket que armazena chaves."""
    def __init__(self, profundidade_local=0, capacidade=4):
        self.profundidade_local = profundidade_local
        self.capacidade = capacidade
        self.chaves = []

    def is_full(self):
        """Verifica se o bucket está cheio."""
        return len(self.chaves) >= self.capacidade
    
    def inserir(self, chave):
        if not self.is_full():
            self.chaves.append(chave)
            return True
        return False

    def __repr__(self):
        return f"Bucket(pL={self.profundidade_local}, chaves={self.chaves})"

class HashingExtensivel:
    """Gerencia o diretório e os buckets."""
    def __init__(self, capacidade_bucket=4):
        self.profundidade_global = 0
        self.capacidade_bucket = capacidade_bucket
        # O diretório é uma lista de referências para os buckets
        self.diretorio = [Bucket(profundidade_local=0, capacidade=capacidade_bucket)]

    def _get_indice_diretorio(self, chave):
        """Calcula o índice no diretório usando a profundidade global."""
        valor_hash = hash_chave(chave)
        # Usa os pG bits menos significativos como índice
        # (Inverter os bits é uma otimização comum, mas vamos simplificar aqui)
        return valor_hash & ((1 << self.profundidade_global) - 1)

    def encontrar_bucket(self, chave):
        """Encontra o bucket correto para uma chave."""
        indice = self._get_indice_diretorio(chave)
        return self.diretorio[indice]
```

---

## Operações (Inserção e Tratamento de Overflow)

Este é o coração do hashing extensível.

### Cenário de Inserção

1.  **Calcular o Hash:** Calcule o valor de hash da chave a ser inserida.
2.  **Consultar o Diretório:** Use os primeiros **pG** (profundidade global) bits do hash como um índice para o diretório.
3.  **Acessar o Bucket:** Siga o ponteiro na entrada do diretório para encontrar o bucket correto.
4.  **Inserir no Bucket:**
    *   **Se o bucket tiver espaço:** Adicione a chave ao bucket. A operação termina.
    *   **Se o bucket estiver cheio (overflow):** A lógica se complica.

### Tratando um Bucket Cheio (Overflow)

A decisão depende da relação entre a profundidade local (pL) do bucket e a profundidade global (pG) do diretório.

#### Caso 1: Profundidade Local < Profundidade Global (pL < pG)

Isso significa que mais de uma entrada do diretório já aponta para este bucket. Podemos dividi-lo sem precisar aumentar o diretório.

1.  **Dividir o Bucket:** Crie um novo bucket vazio.
2.  **Incrementar Profundidades Locais:** Aumente em 1 a `pL` do bucket original e defina a `pL` do novo bucket com o mesmo valor.
3.  **Redistribuir Chaves:** Realoque as chaves do bucket original (incluindo a nova chave) entre o bucket original e o novo.
4.  **Atualizar Diretório:** Altere as entradas do diretório que antes apontavam para o bucket original para que algumas delas agora apontem para o novo bucket.

#### Caso 2: Profundidade Local == Profundidade Global (pL == pG)

O diretório não tem "resolução" suficiente para diferenciar as chaves. Precisamos expandi-lo.

1.  **Dobrar o Diretório:** Primeiro, **dobre o tamanho do diretório**. Para isso, incremente a profundidade global (`pG`) em 1. As entradas existentes são duplicadas.
2.  **Agora, pL < pG:** Após dobrar o diretório, caímos no Caso 1. Prossiga com os passos do Caso 1 para dividir o bucket e atualizar os ponteiros no novo diretório maior.

### Exemplo de Inserção em Python

```python
class HashingExtensivel:
    # ... (código anterior) ...

    def inserir(self, chave):
        bucket = self.encontrar_bucket(chave)

        # Se o bucket tem espaço, insere e termina
        if bucket.inserir(chave):
            print(f"Chave {chave} inserida em {bucket}")
            return

        # Se o bucket está cheio (overflow), trata o overflow
        print(f"Overflow ao tentar inserir {chave} em {bucket}. Dividindo...")
        self._tratar_overflow(bucket, chave)

    def _tratar_overflow(self, bucket, chave_nova):
        # Caso 2: pL == pG -> Precisamos dobrar o diretório primeiro
        if bucket.profundidade_local == self.profundidade_global:
            self._dobrar_diretorio()

        # Dividir o bucket (agora pL < pG com certeza)
        pL_antiga = bucket.profundidade_local
        bucket.profundidade_local += 1
        
        novo_bucket = Bucket(
            profundidade_local=bucket.profundidade_local,
            capacidade=self.capacidade_bucket
        )

        # Redistribuir chaves
        chaves_antigas = bucket.chaves + [chave_nova]
        bucket.chaves = []

        for k in chaves_antigas:
            self.inserir(k) # Reinsere as chaves, agora elas encontrarão o bucket certo

        # Atualizar ponteiros do diretório
        # Encontra o primeiro índice que apontava para o bucket antigo
        # e o "prefixo" que o diferencia do novo bucket
        prefixo_antigo = hash_chave(chaves_antigas[0]) & ((1 << pL_antiga) - 1)
        
        for i in range(len(self.diretorio)):
            if self.diretorio[i] == bucket:
                # Se o bit de ordem pL+1 for 1, aponta para o novo bucket
                if (i >> pL_antiga) & 1:
                    self.diretorio[i] = novo_bucket

    def _dobrar_diretorio(self):
        print(f"Dobrando diretório. Nova pG: {self.profundidade_global + 1}")
        self.profundidade_global += 1
        # Dobra a lista de referências
        self.diretorio = self.diretorio * 2

    def print_estado(self):
        print("="*30)
        print(f"Profundidade Global (pG): {self.profundidade_global}")
        for i, bucket_ref in enumerate(self.diretorio):
            # Imprime o bucket apenas na primeira vez que ele aparece
            if i == 0 or self.diretorio[i] != self.diretorio[i-1]:
                 print(f"Índice {bin(i)[2:].zfill(self.profundidade_global)} -> {bucket_ref}")
        print("="*30)

# --- Exemplo de Uso ---
hashing = HashingExtensivel(capacidade_bucket=2)
hashing.print_estado()

for i in range(10):
    print(f"--- Inserindo chave {i} (hash: {bin(hash_chave(i))}) ---")
    hashing.inserir(i)
    hashing.print_estado()

```

---

## Remoção e Concatenação

O sistema também é dinâmico na remoção.

1.  **Remover a Chave:** Encontre e remova a chave do bucket apropriado.
2.  **Verificar Concatenação:** Após a remoção, verifique se o bucket pode ser combinado com seu "bucket amigo" (o bucket que compartilha o mesmo prefixo, exceto pelo último bit).
3.  **Condições para Concatenar:** A concatenação só pode ocorrer se a soma das chaves nos dois buckets amigos couber em um único bucket.
4.  **Verificar Redução do Diretório:** Após uma concatenação, é possível que o diretório possa ser reduzido pela metade. Isso acontece se **todos** os buckets tiverem uma profundidade local menor que a profundidade global. Se for o caso, a profundidade global é decrementada e o diretório é encolhido.

A implementação da remoção segue uma lógica inversa à da inserção, mas pode se tornar complexa, envolvendo a busca por "buckets amigos" e a verificação de múltiplas condições para encolher as estruturas.

---

## Vantagens

*   **Desempenho Estável:** O desempenho não se degrada significativamente à medida que o arquivo cresce.
*   **Adaptação Dinâmica:** O arquivo cresce e encolhe com as inserções e remoções, evitando o desperdício de espaço ou a necessidade de reorganizações massivas.
*   **Acessos a Disco:** Na maioria dos casos, uma busca requer apenas **dois acessos a disco**: um para o diretório (se não estiver em RAM) e um para o bucket.

Em resumo, o hashing extensível usa uma camada de indireção (o diretório) para permitir que o arquivo de dados (os buckets) cresça de forma granular e sob demanda, resolvendo os principais problemas do hashing estático em memória secundária.
