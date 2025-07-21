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
    def __init__(self, localDepth: int = 0):
        self.__localDepth = localDepth
        self.__keyCounter = 0
        self.__keys = [-1]*TAM_MAX_BUCKET

    def isFull(self):
        """
        Verifica se o bucket está cheio
        """
        return self.__keyCounter == TAM_MAX_BUCKET
    
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
        
    def insert(self, key) -> bool:
        """ 
        Insere uma chave no bucket
        - Retorna True se a inserção for bem-sucedida
        - Retorna False se o bucket estiver cheio
        """
        if self.isFull():
            return False
        
        for i in range(TAM_MAX_BUCKET):
            if self.__keys[i] == -1:
                self.__keys[i] = key
                self.__keyCounter += 1
                return True
        return False
        
    def remove(self, key) -> bool:
        """ 
        Remove uma chave do bucket
        - Retorna True se a remoção for bem-sucedida
        - Retorna False se a chave não for encontrada
        """
        for i in range(TAM_MAX_BUCKET):
            if self.__keys[i] == key:
                self.__keys[i] = -1
                self.__keyCounter -= 1
                return True
        return False
    
    def divide(self) -> 'Bucket':
        """
        Divide o bucket em dois, aumentando a profundidade local
        Retorna o novo bucket criado
        """
        newBucket = Bucket(self.__localDepth + 1)
        half = TAM_MAX_BUCKET // 2
        
        # Move metade das chaves para o novo bucket
        for i in range(half, TAM_MAX_BUCKET):
            if self.__keys[i] != -1:
                newBucket.insert(self.__keys[i])
                self.__keys[i] = -1
                self.__keyCounter -= 1
        
        return newBucket
    
    def info(self) -> tuple[int, int, list]:
        """
        Retorna as informações do Bucket (profundidade, quantidade de chaves, lista de chaves)
        """
        return self.__localDepth, self.__keyCounter, self.__keys

@dataclass
class Ref:
    bucket: Bucket

@dataclass
class Node:
    one: 'Node' | None = None
    zero: 'Node' | None = None

class Directory:
    def __init__(self, globalDepth: int = 0):
        self.__globalDepth = globalDepth
        self.__refs = [-1] * (2 ** globalDepth)