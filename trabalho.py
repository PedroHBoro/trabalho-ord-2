import struct
TAM_MAX_BUCKET=4

class Bucket:
    def __init__(self, depht: int):
        self.keys = [None]*TAM_MAX_BUCKET
        self.keyCount = 0

class Directory:
    def __init__(self, refs: list[int], dirDepht: int):
        pass

class Hashing:
    def __init__(self, arcName: str, dir: Directory):
        self.buckets = open(arcName, 'rb+') # testar com wb tbm

def hashing(key: int, depht: int) -> int:
    result = 0
    mask = 1
    hashVal = key
    for i in range(1, depht):
        result = result << 1
        lowOrderBit = hashVal & mask
        result = result | lowOrderBit
        hashVal = hashVal >> 1
    return result

def search(key: int):
    pass