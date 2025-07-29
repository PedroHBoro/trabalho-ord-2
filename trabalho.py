from hmac import new
from sys import argv
import struct
import os

TAM_MAX_BUCKET = 5
DIR_FILENAME = "diretorio.dat"
BUCKET_FILENAME = "buckets.dat"
BUCKET_SIZE_BYTES = 4 + 4 + (TAM_MAX_BUCKET * 4)

def hashing(key: int, depth: int) -> int:
    result = 0
    mask = 1
    hashVal = key
    for i in range(depth):
        result = result << 1
        lowOrderBit = hashVal & mask
        result = result | lowOrderBit
        hashVal = hashVal >> 1
    return result

class Bucket:
    def __init__(self, bucketsArchive, rrn: int, localDepth: int = 0):
        self.rrn = rrn
        self.localDepth = localDepth
        self.keys = [-1]*TAM_MAX_BUCKET
        self.keyCounter = 0
        self.__dat = bucketsArchive

    def isFull(self) -> bool:
        """
        Verifica se o bucket está cheio
        """
        return self.keyCounter >= TAM_MAX_BUCKET

    def search(self, key) -> tuple[bool, 'Bucket', int]:
        """
        Retorna o resultado da busca (sucesso, bucket, índice)
        """
        try:
            index = self.keys.index(key)
            foundKey = self.keys[index]

            return True, self, index
        except:
            return False, self, -1

    def insert(self, key) -> tuple[bool, 'Bucket']:
        """ 
        Insere uma chave no bucket
        """
        for i in range(TAM_MAX_BUCKET):
            if self.keys[i] == -1:
                self.keys[i] = key
                self.keyCounter += 1
                return True, self.save()
        return False, self

    def remove(self, key) -> tuple[bool, 'Bucket']:
        """ 
        Remove uma chave do bucket
        - Retorna True se a remoção for bem-sucedida
        - Retorna False se a chave não for encontrada
        """
        for i in range(TAM_MAX_BUCKET):
            if self.keys[i] == key:
                self.keys[i] = -1
                self.keyCounter -= 1
                return True, self.save()
        return False, self

    def save(self) -> 'Bucket':
        """
        Salva o estado do bucket no arquivo
        """
        format = f'ii{TAM_MAX_BUCKET}i'
        seekLocation = self.rrn * BUCKET_SIZE_BYTES

        self.__dat.seek(seekLocation)
        self.__dat.write(struct.pack(format, self.localDepth, self.keyCounter, *self.keys))

        return self


class Directory:
    def __init__(self):
        self.globalDepth = 0
        self.refs = []
        self.numBuckets = 0
        self.refs = [0] * (2 ** self.globalDepth)

        # Abre ou cria o arquivo de buckets
        if not os.path.exists(BUCKET_FILENAME):
            open(BUCKET_FILENAME, 'wb').close()
        
        self.__bucketsArchive = open(BUCKET_FILENAME, 'rb+')
        
        # Inicializa com um bucket
        if os.path.getsize(BUCKET_FILENAME) == 0:
            bucket = Bucket(self.__bucketsArchive, 0, localDepth=0)
            bucket.save()
            self.numBuckets = 1
        else:
            self.numBuckets = os.path.getsize(BUCKET_FILENAME) // BUCKET_SIZE_BYTES

    def _load_bucket(self, rrn: int) -> Bucket:
        """
        Carrega um bucket do arquivo de buckets usando o RRN informado.
        Retorna uma instância de Bucket preenchida com os dados lidos.
        """
        self.__bucketsArchive.seek(rrn * BUCKET_SIZE_BYTES)
        data = self.__bucketsArchive.read(BUCKET_SIZE_BYTES)
        
        format = f'ii{TAM_MAX_BUCKET}i'
        localDepth, keyCounter, *keys = struct.unpack(format, data)

        bucket = Bucket(self.__bucketsArchive, rrn, localDepth)
        bucket.keys = list(keys)
        bucket.keyCounter = keyCounter

        return bucket

    def search(self, key: int) -> tuple[bool, Bucket | None]:
        """
        Busca uma chave no diretório.
        Retorna uma tupla (encontrado, bucket) onde encontrado é True se a chave foi localizada.
        Se o bucket não for encontrado (ponteiro -1), retorna (False, None).
        """
        hash_val = hashing(key, self.globalDepth)

        bucket_rrn = self.refs[hash_val]
        if bucket_rrn == -1:
            return False, None
            
        bucket = self._load_bucket(bucket_rrn)
        found, _, _ = bucket.search(key)

        return found, bucket

    def insert(self, key: int) -> bool:
        """
        Insere uma chave no diretório.
        Se a chave já existir, retorna False. Caso contrário, insere e retorna True.
        Se o bucket estiver cheio, realiza split e tenta novamente.
        """
        found, bucket = self.search(key)
        if found:
            return False
        
        # Se a busca não encontrou um bucket válido (ponteiro -1), a inserção falha.
        if bucket is None:
            return False

        if not bucket.isFull():
            return bucket.insert(key)[0]
        
        # Se o bucket está cheio, realiza o split e tenta novamente.
        self.split_bucket(bucket)
        return self.insert(key)

    def remove(self, key: int) -> bool:
        """
        Remove uma chave do diretório.
        Retorna True se a remoção for bem-sucedida, False caso contrário.
        Após remoção, tenta concatenar buckets amigos.
        """
        found, bucket = self.search(key)
        # Se a chave não foi encontrada ou o bucket não existe (ponteiro -1), a remoção falha.
        if not found or bucket is None:
            return False
            
        success, _ = bucket.remove(key)
        if success:
            self.try_merge_buckets(bucket)
        return success

    def split_bucket(self, bucket: Bucket) -> 'Directory':
        """
        Realiza o split de um bucket cheio, criando um novo bucket e redistribuindo as chaves.
        Atualiza as referências do diretório conforme a profundidade local.
        """

        # Dobra o diretório se a profundidade local do bucket for igual à profundidade global
        if bucket.localDepth == self.globalDepth:
            self.double()

        # Cria um novo bucket
        new_bucket_rrn = self.numBuckets
        self.numBuckets += 1
        
        bucket.localDepth += 1
        new_bucket = Bucket(self.__bucketsArchive, new_bucket_rrn, bucket.localDepth)

        # Guarda as chaves antigas
        old_keys = [key for key in bucket.keys if key != -1]

        # Limpa o bucket original
        bucket.keys = [-1] * TAM_MAX_BUCKET
        bucket.keyCounter = 0

        # Atualiza as referências no diretório (se baseando no bit menos significativo do hash)
        mask = 1
        
        for i in range(len(self.refs)):
            if self.refs[i] == bucket.rrn:
                if i & mask:
                    self.refs[i] = new_bucket_rrn

        # Redistribui as chaves entre o bucket antigo e o novo (se baseando no bit menos significativo do hash)
        for key in old_keys:
            hash_val = hashing(key, bucket.localDepth)
            if hash_val & mask:
                new_bucket.insert(key)
            else:
                bucket.insert(key)
        
        bucket.save()
        new_bucket.save()

        return self

    def double(self) -> 'Directory':
        """
        Dobra a profundidade global do diretório e duplica as referências dos buckets.
        """
        self.globalDepth += 1
        newRefs = []
        for ref in self.refs:
            newRefs.extend([ref, ref])
        self.refs = newRefs
        return self

    def try_merge_buckets(self, bucket: Bucket):
        """
        Tenta concatenar o bucket com seu bucket amigo após uma remoção.
        Se possível, move todas as chaves para um bucket, marca o outro como inativo (localDepth = -1),
        atualiza as referências do diretório e reduz a profundidade local.
        """
        if bucket.localDepth <= 0:
            return False

        mask = 1
        
        # Encontra um índice que aponta para o bucket atual
        my_index = -1
        for i, ref in enumerate(self.refs):
            if ref == bucket.rrn:
                my_index = i
                break

        # Encontra o bucket amigo
        buddy_index = my_index ^ mask
        buddy_rrn = self.refs[buddy_index]

        if buddy_rrn == bucket.rrn: 
            return False # Aponta para si mesmo, não tem amigo

        buddy = self._load_bucket(buddy_rrn)

        # Verifica as condições de concatenação
        if buddy.localDepth != bucket.localDepth:
            return False

        if bucket.keyCounter + buddy.keyCounter > TAM_MAX_BUCKET:
            return False # Não cabe

        # Executa a concatenação: move chaves para o buddy e desativa o bucket atual
        for key in bucket.keys:
            if key != -1:
                buddy.insert(key)
        
        buddy.localDepth -= 1
        bucket.localDepth = -1 # Marca como inativo
        bucket.keyCounter = 0
        bucket.keys = [-1] * TAM_MAX_BUCKET
        self.numBuckets -= 1

        bucket.save()
        buddy.save()

        # Atualiza as referências do diretório
        for i in range(len(self.refs)):
            if self.refs[i] == bucket.rrn:
                self.refs[i] = buddy.rrn

        # Tenta reduzir o diretório
        reduce = self.try_reduce_directory()
        if reduce:
            self.try_merge_buckets(buddy)

    def try_reduce_directory(self):
        """
        Tenta reduzir a profundidade global se todos os buckets ativos tiverem profundidade local < global.
        """
        if self.globalDepth <= 0:
            return False

        # Verifica se todos os buckets ativos têm profundidade local menor que a global
        all_buckets = self.list_buckets()
        if not all(bucket.localDepth < self.globalDepth for bucket in all_buckets):
            return False

        new_refs = []
        for i in range(0, len(self.refs), 2):
            new_refs.append(self.refs[i])

        self.refs = new_refs
        self.globalDepth -= 1

        return True

    def list_buckets(self) -> list[Bucket]:
        """
        Lista todos os buckets no diretório.
        Retorna uma lista de instâncias de Bucket.
        """
        bucketRRNs = set(self.refs)
        return [self._load_bucket(rrn) for rrn in bucketRRNs]

    def save(self) -> 'Directory':
        """
        Salva o estado do diretório no arquivo especificado por DIR_FILENAME.
        Armazena a profundidade global e os RRNs dos buckets.
        """
        with open(DIR_FILENAME, 'wb') as f:
            f.write(struct.pack('i', self.globalDepth))
            for ref in self.refs:
                f.write(struct.pack('i', ref))
        return self

    def load(self) -> 'Directory':
        """
        Carrega o estado do diretório do arquivo especificado por DIR_FILENAME.
        Recupera a profundidade global e o vetor de RRNs dos buckets.
        """
        with open(DIR_FILENAME, 'rb') as f:
            self.globalDepth = struct.unpack('i', f.read(4))[0]
            self.refs = []
            for _ in range(2 ** self.globalDepth):
                self.refs.append(struct.unpack('i', f.read(4))[0])
        return self

    def close(self):
        """
        Fecha o arquivo de buckets.
        """
        if self.__bucketsArchive and not self.__bucketsArchive.closed:
            self.__bucketsArchive.close()


class Hashing():
    def __init__(self):
        if os.path.exists(DIR_FILENAME):
            self.directory = Directory().load()
        else:
            self.directory = Directory()

    def insert(self, key: int) -> bool:
        return self.directory.insert(key)

    def search(self, key: int) -> tuple[bool, Bucket | None]:
        return self.directory.search(key)

    def remove(self, key: int) -> bool:
        return self.directory.remove(key)

    def close(self):
        """
        Fecha os arquivos abertos.
        """
        self.directory.close()

    def execute(self, command: str, key: int) -> bool:
        if command == "i":
            success = self.insert(key)
            print(f"Inserção da chave {key}: {'Sucesso' if success else 'Falha - Chave duplicada'}.")
            return success
        elif command == "b":
            success, bucket = self.search(key)
            print(f"Busca pela chave {key}: {f'Chave encontrada no bucket {bucket.rrn}' if success else 'Chave não encontrada'}.")
            return success
        elif command == "r":
            success = self.remove(key)
            print(f"Remoção da chave {key}: {'Sucesso' if success else 'Falha - Chave não encontrada'}.")
            return success
        else:
            return False
        
    def print_directory(self):
        print("----- Diretório -----")
        for index, ref in enumerate(self.directory.refs):
            print(f"dir[{index}] = bucket[{ref}]")

        print(
            f"\nProfundidade = {self.directory.globalDepth}\n"
            f"Tamanho atual = {len(self.directory.refs)}\n"
            f"Total de buckets = {self.directory.numBuckets}\n"
        )

    def print_buckets(self):
        buckets = self.directory.list_buckets()
        print("----- Buckets -----")

        if not buckets:
            print("Nenhum bucket encontrado.")
            return

        for bucket in buckets:
            if bucket.localDepth == -1:
                print(f"Bucket {bucket.rrn} --> Removido")
                continue
            
            print(
                f"\nBucket {bucket.rrn} (prof = {bucket.localDepth})\n"
                f"Conta_chaves = {bucket.keyCounter}\n"
                f"Chaves = {bucket.keys}\n"
            )

    def save(self) -> 'Hashing':
        self.directory.save()
        return self
    
def main():
    hashing = Hashing()
    try:
        flag = argv[1]

        if flag == "-pb":
            hashing.print_buckets()

        elif flag == "-pd":
            hashing.print_directory()

        elif flag == "-e":
            opAchiveName = argv[2]
            with open(opAchiveName, 'r') as operations:
                for line in operations:
                    command, key = line.strip().split()
                    key = int(key)
                    hashing.execute(command, key)
            hashing.save()
    finally:
        hashing.close()

if __name__ == "__main__":
    main()