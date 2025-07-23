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
        self.__keyCounter = 0
        self.__keys = [-1]*TAM_MAX_BUCKET
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
            index = self.__keys.index(key)
            foundKey = self.__keys[index]

            return True, self, index
        except:
            return False, self, -1
        
    def insert(self, key) -> 'Bucket':
        """ 
        Insere uma chave no bucket
        """
        for i in range(TAM_MAX_BUCKET):
            if self.__keys[i] == -1:
                self.__keys[i] = key
                self.__keyCounter += 1
                return self.__save()

    def remove(self, key) -> tuple[bool, 'Bucket']:
        """ 
        Remove uma chave do bucket
        - Retorna True se a remoção for bem-sucedida
        - Retorna False se a chave não for encontrada
        """
        for i in range(TAM_MAX_BUCKET):
            if self.__keys[i] == key:
                self.__keys[i] = -1
                self.__keyCounter -= 1
                return True, self.__save()
        return False, self
    
    def __save(self) -> 'Bucket':
        """
        Salva o estado do bucket no arquivo
        """

        format = f'ii{TAM_MAX_BUCKET}i'
        seekLocation = self.__rrn * BUCKET_SIZE_BYTES

        self.__dat.seek(seekLocation)
        self.__dat.write(struct.pack(format, self.localDepth, self.__keyCounter, *self.__keys))

        return self

class Directory:
    def __init__(self, globalDepth: int = 0):
        self.__globalDepth = globalDepth
        self.__refs = [-1] * (2 ** globalDepth)
        self.__numBuckets = 0
        self.__bucketsArchive = open(BUCKET_FILENAME, 'wb+')

    def search(self, key: int) -> tuple[bool, Bucket]:
        """
        Busca uma chave no diretório
        retorna uma tupla (encontrado, bucket, índice)
        """
        hash = hashing(key, self.__globalDepth)
        bucket = self.__refs[hash]

        (success, bucket, index) = bucket.search(key)
        
        return success, bucket

    def insert(self, key: int) -> bool:
        """
        Insere uma chave no diretório
        """
        found, bucket = self.search(key)
        if found:
            return False

        if not bucket.isFull():
            bucket.insert(key)
            return True
        
        self.splitBucket(bucket)

    def remove(self, key: int) -> bool:
        """
        Remove uma chave do diretório
        Retorna True se a remoção for bem-sucedida, False caso contrário
        """
        pass

    def splitBucket(self, bucket: Bucket) -> 'Directory':
        """
        Divide um bucket e atualiza o diretório
        """
        if bucket.localDepth == self.__globalDepth:
            self.double()
        
        # dividir e reorganizar
    
    def double(self) -> 'Directory':
        """
        Dobra a profundidade global do diretório
        """
        self.__globalDepth += 1
        newRefs = [-1] * (2 ** self.__globalDepth)
        for i in range(len(self.__refs)):
            newRefs[i] = self.__refs[i]
        self.__refs = newRefs
        return self

    def __save(self) -> 'Directory':
        """
        Salva o estado do diretório no arquivo
        """
        pass

    def __load(self) -> 'Directory':
        """
        Carrega o estado do diretório do arquivo e os buckets associados
        """
        pass



    # Como salvar diretorio em arquivo?