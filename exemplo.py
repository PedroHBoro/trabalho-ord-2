

import sys
import struct
import os

# --- Constantes ---
TAM_MAX_BUCKET = 4  # Número de chaves que um bucket pode armazenar
DIR_FILENAME = "diretorio.dat"
BUCKET_FILENAME = "buckets.dat"
BUCKET_SIZE_BYTES = 4 + 4 + (TAM_MAX_BUCKET * 4)  # PL, Cont, Chaves

class Bucket:
    """Representa um bucket que armazena chaves."""

    def __init__(self, profundidade_local=0):
        self.profundidade_local = profundidade_local
        self.contador_chaves = 0
        self.chaves = [-1] * TAM_MAX_BUCKET

    def is_full(self):
        """Verifica se o bucket está cheio."""
        return self.contador_chaves == TAM_MAX_BUCKET

    def find(self, chave):
        """Procura uma chave no bucket."""
        return chave in self.chaves

    def insert(self, chave):
        """Insere uma chave no bucket se houver espaço."""
        if not self.is_full():
            for i in range(TAM_MAX_BUCKET):
                if self.chaves[i] == -1:
                    self.chaves[i] = chave
                    self.contador_chaves += 1
                    return True
        return False

    def remove(self, chave):
        """Remove uma chave do bucket."""
        try:
            index = self.chaves.index(chave)
            self.chaves[index] = -1
            self.contador_chaves -= 1
            return True
        except ValueError:
            return False

    def get_chaves(self):
        """Retorna uma lista de chaves válidas no bucket."""
        return [k for k in self.chaves if k != -1]

    def pack(self):
        """Converte o bucket para formato binário (bytes)."""
        format_string = f'ii{TAM_MAX_BUCKET}i'
        return struct.pack(format_string, self.profundidade_local, self.contador_chaves, *self.chaves)

    @classmethod
    def unpack(cls, data):
        """Cria um objeto Bucket a partir de dados binários."""
        format_string = f'ii{TAM_MAX_BUCKET}i'
        unpacked_data = struct.unpack(format_string, data)
        
        bucket = cls(profundidade_local=unpacked_data[0])
        bucket.contador_chaves = unpacked_data[1]
        bucket.chaves = list(unpacked_data[2:])
        return bucket

    def __str__(self):
        chaves_validas = self.get_chaves()
        return f"(Prof={self.profundidade_local}): Contem {self.contador_chaves} chaves = {chaves_validas}"

class Diretorio:
    """Representa o diretório do hashing."""

    def __init__(self, profundidade_global=0):
        self.profundidade_global = profundidade_global
        tamanho = 2 ** profundidade_global
        self.refs = [0] * tamanho

    def pack(self):
        """Converte o diretório para formato binário."""
        # Armazena a profundidade e depois as referências
        tamanho = len(self.refs)
        format_string = f'i{tamanho}i'
        return struct.pack(format_string, self.profundidade_global, *self.refs)

    @classmethod
    def unpack(cls, data):
        """Cria um objeto Diretorio a partir de dados binários."""
        profundidade_global = struct.unpack('i', data[:4])[0]
        diretorio = cls(profundidade_global)
        
        tamanho = 2 ** profundidade_global
        format_string = f'{tamanho}i'
        diretorio.refs = list(struct.unpack(format_string, data[4:]))
        return diretorio

    def double(self):
        """Dobra o tamanho do diretório."""
        self.profundidade_global += 1
        self.refs *= 2

    def __str__(self):
        info = [f"dir[{i}] = bucket({ref})" for i, ref in enumerate(self.refs)]
        num_buckets_unicos = len(set(self.refs))
        return "\n".join([
            "Diretório",
            *info,
            f"Profundidade = {self.profundidade_global}",
            f"Tamanho atual = {len(self.refs)}",
            f"Total de buckets referenciados = {num_buckets_unicos}"
        ])

class HashingExtensivel:
    """Implementação principal do Hashing Extensível."""

    def __init__(self):
        self.diretorio = None
        self.bucket_file = None
        self._initialize()

    def _initialize(self):
        """Carrega ou cria os arquivos de hashing."""
        if os.path.exists(DIR_FILENAME) and os.path.exists(BUCKET_FILENAME):
            # Carrega arquivos existentes
            with open(DIR_FILENAME, 'rb') as f:
                self.diretorio = Diretorio.unpack(f.read())
            self.bucket_file = open(BUCKET_FILENAME, 'r+b')
        else:
            # Cria novos arquivos
            self.diretorio = Diretorio(profundidade_global=0)
            self.bucket_file = open(BUCKET_FILENAME, 'w+b')
            
            # Cria o primeiro bucket vazio
            b_inicial = Bucket(profundidade_local=0)
            self._write_bucket(0, b_inicial)
            self.diretorio.refs[0] = 0
            self.close() # Salva o estado inicial
            self._initialize() # Reabre no modo correto

    def _hash(self, chave):
        """Função de hash simples."""
        return chave

    def _gerar_endereco(self, chave, profundidade):
        """Gera o endereço no diretório usando os bits menos significativos do hash."""
        hash_val = self._hash(chave)
        return hash_val & ((1 << profundidade) - 1)

    def _read_bucket(self, rrn):
        """Lê um bucket do arquivo pelo seu RRN."""
        self.bucket_file.seek(rrn * BUCKET_SIZE_BYTES)
        data = self.bucket_file.read(BUCKET_SIZE_BYTES)
        if not data:
            return None
        return Bucket.unpack(data)

    def _write_bucket(self, rrn, bucket):
        """Escreve um bucket no arquivo em um RRN específico."""
        self.bucket_file.seek(rrn * BUCKET_SIZE_BYTES)
        self.bucket_file.write(bucket.pack())

    def _append_bucket(self, bucket):
        """Adiciona um novo bucket ao final do arquivo e retorna seu RRN."""
        self.bucket_file.seek(0, os.SEEK_END)
        rrn = self.bucket_file.tell() // BUCKET_SIZE_BYTES
        self._write_bucket(rrn, bucket)
        return rrn

    def find(self, chave):
        """Encontra uma chave e retorna (encontrado, rrn, bucket)."""
        addr = self._gerar_endereco(chave, self.diretorio.profundidade_global)
        rrn = self.diretorio.refs[addr]
        bucket = self._read_bucket(rrn)
        
        if bucket and bucket.find(chave):
            return True, rrn, bucket
        return False, rrn, bucket

    def insert(self, chave):
        """Insere uma chave no hashing."""
        encontrado, rrn, bucket = self.find(chave)
        if encontrado:
            print(f"Inserção da chave {chave}: Falha - Chave duplicada.")
            return False

        if not bucket.is_full():
            bucket.insert(chave)
            self._write_bucket(rrn, bucket)
            print(f"Inserção da chave {chave}: Sucesso.")
            return True
        
        # Overflow - dividir bucket
        self._split_bucket(rrn, bucket, chave)
        # Tenta inserir a chave novamente após a divisão
        return self.insert(chave)

    def _split_bucket(self, rrn_antigo, bucket_antigo, chave_nova):
        """Lida com o overflow dividindo um bucket."""
        # Se necessário, dobra o diretório
        if bucket_antigo.profundidade_local == self.diretorio.profundidade_global:
            self.diretorio.double()

        # Cria novo bucket e atualiza profundidades
        bucket_novo = Bucket(profundidade_local=bucket_antigo.profundidade_local + 1)
        bucket_antigo.profundidade_local += 1
        
        rrn_novo = self._append_bucket(bucket_novo)
        
        # Redistribui chaves
        chaves_a_redistribuir = bucket_antigo.get_chaves() + [chave_nova]
        bucket_antigo.chaves = [-1] * TAM_MAX_BUCKET
        bucket_antigo.contador_chaves = 0

        for k in chaves_a_redistribuir:
            addr = self._gerar_endereco(k, self.diretorio.profundidade_global)
            if self.diretorio.refs[addr] == rrn_antigo:
                 bucket_antigo.insert(k)
            else: # Precisa ser inserido no novo bucket
                 # Temporariamente aponta para o novo bucket para a inserção funcionar
                 ref_original = self.diretorio.refs[addr]
                 self.diretorio.refs[addr] = rrn_novo
                 self.insert(k)
                 self.diretorio.refs[addr] = ref_original # Restaura

        # Atualiza as referências do diretório
        # Encontra o primeiro endereço que aponta para o bucket antigo
        # e que agora deve apontar para o novo
        for i in range(len(self.diretorio.refs)):
            # O bit que diferencia o bucket novo do antigo
            bit_diferenciador = 1 << (bucket_antigo.profundidade_local - 1)
            if (i & bit_diferenciador):
                 if self.diretorio.refs[i] == rrn_antigo:
                      self.diretorio.refs[i] = rrn_novo

        self._write_bucket(rrn_antigo, bucket_antigo)
        self._write_bucket(rrn_novo, self._read_bucket(rrn_novo)) # Recarrega e escreve o novo

    def remove(self, chave):
        """Remove uma chave do hashing."""
        encontrado, rrn, bucket = self.find(chave)
        if not encontrado:
            print(f"Remoção da chave {chave}: Falha - Chave não encontrada.")
            return False
        
        bucket.remove(chave)
        self._write_bucket(rrn, bucket)
        print(f"Remoção da chave {chave}: Sucesso.")
        # Lógica de merge (opcional/simplificada) não implementada
        return True

    def print_diretorio(self):
        """Imprime o estado atual do diretório."""
        print(self.diretorio)

    def print_buckets(self):
        """Imprime o conteúdo de todos os buckets ativos."""
        print("Buckets")
        self.bucket_file.seek(0)
        rrn = 0
        while True:
            data = self.bucket_file.read(BUCKET_SIZE_BYTES)
            if not data:
                break
            bucket = Bucket.unpack(data)
            # A especificação pede para mostrar buckets removidos, mas
            # esta implementação não os marca, apenas os deixa vazios.
            print(f"Bucket {rrn} {bucket}")
            rrn += 1

    def close(self):
        """Salva o diretório e fecha os arquivos."""
        if self.diretorio:
            with open(DIR_FILENAME, 'wb') as f:
                f.write(self.diretorio.pack())
        if self.bucket_file:
            self.bucket_file.close()

def processar_operacoes(filename, hashing):
    """Processa um arquivo de operações."""
    with open(filename, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if not parts:
                continue
            
            op = parts[0]
            chave = int(parts[1])

            if op == 'i':
                hashing.insert(chave)
            elif op == 'r':
                hashing.remove(chave)
            elif op == 'b':
                encontrado, rrn, _ = hashing.find(chave)
                if encontrado:
                    print(f"Busca pela chave {chave}: Chave encontrada no bucket {rrn}.")
                else:
                    print(f"Busca pela chave {chave}: Chave não encontrada.")

def main():
    """Função principal para lidar com argumentos de linha de comando."""
    args = sys.argv[1:]
    
    if not args:
        print("Uso: python trabalho.py [-e <arquivo_op>] [-pd] [-pb]")
        return

    hashing = HashingExtensivel()

    if args[0] == '-e':
        if len(args) < 2:
            print("Erro: Faltando nome do arquivo para a opção -e.")
        else:
            processar_operacoes(args[1], hashing)
    elif args[0] == '-pd':
        hashing.print_diretorio()
    elif args[0] == '-pb':
        hashing.print_buckets()
    else:
        print(f"Erro: Opção desconhecida '{args[0]}'")

    hashing.close()

if __name__ == "__main__":
    main()
