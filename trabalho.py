import sys
import struct
import os

TAM_MAX_BUCKET = 4 
DIR_FILENAME = "diretorio.dat"
BUCKET_FILENAME = "buckets.dat"
BUCKET_SIZE_BYTES = 4 + 4 + (TAM_MAX_BUCKET * 4)

def hashing(key: int, depth: int) -> int:
    result = 0
    mask = 1
    hashVal = key
    for i in range(1, depth):
        result = result << 1
        lowOrderBit = hashVal & mask
        result = result | lowOrderBit
        hashVal = hashVal >> 1
    return result

class Bucket:
    def __init__(self, bucketsArchive, rrn: int, localDepth: int = 0):
        self.id = rrn
        self.localDepth = localDepth
        self.keys = [-1]*TAM_MAX_BUCKET
        self.__keyCounter = 0
        self.__dat = bucketsArchive
        self.__rrn = rrn

    def isFull(self) -> bool:
        """
        Verifica se o bucket está cheio
        """
        return self.__keyCounter >= TAM_MAX_BUCKET

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
        
    def insert(self, key) -> 'Bucket':
        """ 
        Insere uma chave no bucket
        """
        for i in range(TAM_MAX_BUCKET):
            if self.keys[i] == -1:
                self.keys[i] = key
                self.__keyCounter += 1
                return self.__save()

    def remove(self, key) -> tuple[bool, 'Bucket']:
        """ 
        Remove uma chave do bucket
        - Retorna True se a remoção for bem-sucedida
        - Retorna False se a chave não for encontrada
        """
        for i in range(TAM_MAX_BUCKET):
            if self.keys[i] == key:
                self.keys[i] = -1
                self.__keyCounter -= 1
                return True, self.__save()
        return False, self
    
    def set_key_counter(self, value: int):
        """
        Define o contador de chaves com segurança
        """
        if value < 0 or value > TAM_MAX_BUCKET:
            raise ValueError("Contador de chaves inválido")
        self.__keyCounter = value

    def __save(self) -> 'Bucket':
        """
        Salva o estado do bucket no arquivo
        """

        format = f'ii{TAM_MAX_BUCKET}i'
        seekLocation = self.__rrn * BUCKET_SIZE_BYTES

        self.__dat.seek(seekLocation)
        self.__dat.write(struct.pack(format, self.localDepth, self.__keyCounter, *self.keys))

        return self

class Directory:
    def __init__(self, globalDepth: int = 1):
        self.__globalDepth = globalDepth
        self.__refs = []
        self.__numBuckets = 0
        # Abre ou cria o arquivo de buckets
        if not os.path.exists(BUCKET_FILENAME):
            open(BUCKET_FILENAME, 'wb').close()
        self.__bucketsArchive = open(BUCKET_FILENAME, 'rb+')
        # Inicializa com um bucket
        if os.path.getsize(BUCKET_FILENAME) == 0:
            bucket = Bucket(self.__bucketsArchive, 0, localDepth=1)
            bucket.__save()
            self.__refs = [0] * (2 ** self.__globalDepth)
            self.__numBuckets = 1
        else:
            self.__refs = [0] * (2 ** self.__globalDepth)
            self.__numBuckets = os.path.getsize(BUCKET_SIZE_BYTES) // BUCKET_SIZE_BYTES

    def _load_bucket(self, rrn: int) -> Bucket:
        """
        Carrega um bucket do arquivo de buckets usando o RRN informado.
        Retorna uma instância de Bucket preenchida com os dados lidos.
        """
        self.__bucketsArchive.seek(rrn * BUCKET_SIZE_BYTES)
        data = self.__bucketsArchive.read(BUCKET_SIZE_BYTES)
        if not data or len(data) < BUCKET_SIZE_BYTES:
            raise Exception("Bucket não encontrado")
        format = f'ii{TAM_MAX_BUCKET}i'
        localDepth, keyCounter, *keys = struct.unpack(format, data)
        bucket = Bucket(self.__bucketsArchive, rrn, localDepth)
        bucket.keys = list(keys)
        bucket.set_key_counter(keyCounter)
        return bucket

    def search(self, key: int) -> tuple[bool, Bucket]:
        """
        Busca uma chave no diretório.
        Retorna uma tupla (encontrado, bucket) onde encontrado é True se a chave foi localizada.
        """
        hash_val = hashing(key, self.__globalDepth)
        bucket_rrn = self.__refs[hash_val]
        if bucket_rrn == -1:
            # Retorna um bucket "vazio" para manter o tipo
            return False, Bucket(self.__bucketsArchive, -1, 0)
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
        if not bucket.isFull():
            bucket.insert(key)
            return True
        self.splitBucket(bucket)
        return self.insert(key)  # tenta novamente após split

    def remove(self, key: int) -> bool:
        """
        Remove uma chave do diretório.
        Retorna True se a remoção for bem-sucedida, False caso contrário.
        """
        found, bucket = self.search(key)
        if not found or not bucket:
            return False
        success, _ = bucket.remove(key)
        return success

    def splitBucket(self, bucket: Bucket) -> 'Directory':
        """
        Realiza o split de um bucket cheio, criando um novo bucket e redistribuindo as chaves.
        Atualiza as referências do diretório conforme a profundidade local.
        """
        if bucket.localDepth == self.__globalDepth:
            self.double()
        old_rrn = bucket.id
        bucket.localDepth += 1
        new_bucket_rrn = self.__numBuckets
        new_bucket = Bucket(self.__bucketsArchive, new_bucket_rrn, bucket.localDepth)
        self.__numBuckets += 1

        # Atualiza referências do diretório
        for i in range(len(self.__refs)):
            if self.__refs[i] == old_rrn:
                if (i >> (bucket.localDepth - 1)) & 1:
                    self.__refs[i] = new_bucket_rrn

        # Redistribui as chaves
        old_keys = [k for k in bucket.keys if k != -1]
        bucket.keys = [-1]*TAM_MAX_BUCKET
        bucket.set_key_counter(0)
        new_bucket.keys = [-1]*TAM_MAX_BUCKET
        new_bucket.set_key_counter(0)
        bucket.__save()
        new_bucket.__save()
        for key in old_keys:
            self.insert(key)
        return self

    def double(self) -> 'Directory':
        """
        Dobra a profundidade global do diretório e duplica as referências dos buckets.
        """
        self.__globalDepth += 1
        newRefs = []
        for ref in self.__refs:
            newRefs.extend([ref, ref])
        self.__refs = newRefs
        return self

    def __save(self) -> 'Directory':
        """
        Salva o estado do diretório no arquivo especificado por DIR_FILENAME.
        Armazena a profundidade global e o vetor de RRNs dos buckets.
        """
        with open(DIR_FILENAME, 'wb') as f:
            f.write(struct.pack('i', self.__globalDepth))
            for ref in self.__refs:
                f.write(struct.pack('i', ref))
        return self

    def __load(self) -> 'Directory':
        """
        Carrega o estado do diretório do arquivo especificado por DIR_FILENAME.
        Recupera a profundidade global e o vetor de RRNs dos buckets.
        """
        with open(DIR_FILENAME, 'rb') as f:
            self.__globalDepth = struct.unpack('i', f.read(4))[0]
            self.__refs = []
            for _ in range(2 ** self.__globalDepth):
                self.__refs.append(struct.unpack('i', f.read(4))[0])
        return self



    # Como salvar diretorio em arquivo?