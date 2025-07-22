import sys
import struct
import os
from dataclasses import dataclass

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
        self.__localDepth = localDepth
        self.__keyCounter = 0
        self.__keys = [-1]*TAM_MAX_BUCKET
        self.__dat = bucketsArchive
        self.__rrn = rrn

    def search(self, key) -> tuple[bool, int, int]:
        """
        Retorna o resultado da busca (sucesso, índice, valor)
        """
        try:
            index = self.__keys.index(key)
            foundKey = self.__keys[index]

            return True, index, foundKey
        except:
            return False, -1, -1
        
    def insert(self, key) -> 'Bucket':
        """ 
        Insere uma chave no bucket
        - Retorna True se a inserção for bem-sucedida
        - Retorna False se o bucket estiver cheio
        """
        for i in range(TAM_MAX_BUCKET):
            if self.__keys[i] == -1:
                self.__keys[i] = key
                self.__keyCounter += 1
                return self.__save()
        return self.__divide() # Como reorganizar as chaves?

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

    def __divide(self) -> 'Bucket':
        """
        Divide o bucket em dois, aumentando a profundidade local
        Retorna o novo bucket criado
        """
        self.__localDepth += 1

        self.__dat.seek(0, os.SEEK_END)
        newRrn = self.__dat.tell()

        newBucket = Bucket(bucketsArchive=self.__dat, rrn=newRrn, localDepth=self.__localDepth)

        return newBucket.__save()
    
    def __save(self) -> 'Bucket':
        """
        Salva o estado do bucket no arquivo
        """

        format = f'ii{TAM_MAX_BUCKET}i'
        seekLocation = self.__rrn * BUCKET_SIZE_BYTES

        self.__dat.seek(seekLocation)
        self.__dat.write(struct.pack(format, self.__localDepth, self.__keyCounter, *self.__keys))

        return self

@dataclass
class Ref:
    bucket: Bucket

class Directory:
    def __init__(self, globalDepth: int = 0):
        self.__globalDepth = globalDepth
        self.__refs = [-1] * (2 ** globalDepth)
        self.__dat = open(DIR_FILENAME, 'wb+')
        self.__bucketsArchive = open(BUCKET_FILENAME, 'wb+')

    def search(self, key: int) -> tuple[bool, Ref]:
        """
        Busca uma chave no diretório
        Retorna (True, Ref) se encontrado, (False, None) caso contrário
        """
        pass

    def insert(self, key: int) -> Ref:
        """
        Insere uma chave no diretório
        Retorna uma referência ao bucket onde a chave foi inserida
        """
        pass

    def remove(self, key: int) -> bool:
        """
        Remove uma chave do diretório
        Retorna True se a remoção for bem-sucedida, False caso contrário
        """
        pass

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