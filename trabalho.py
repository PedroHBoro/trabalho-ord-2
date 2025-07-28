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
        self.rrn = rrn
        self.localDepth = localDepth
        self.keys = [-1]*TAM_MAX_BUCKET
        self.__keyCounter = 0
        self.__dat = bucketsArchive

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
                return self.save()

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
                return True, self.save()
        return False, self
    
    def set_key_counter(self, value: int):
        """
        Define o contador de chaves com segurança
        """
        if value < 0 or value > TAM_MAX_BUCKET:
            raise ValueError("Contador de chaves inválido")
        self.__keyCounter = value

    def save(self) -> 'Bucket':
        """
        Salva o estado do bucket no arquivo
        """

        format = f'ii{TAM_MAX_BUCKET}i'
        seekLocation = self.rrn * BUCKET_SIZE_BYTES

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
            bucket.save()
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
        Após remoção, tenta concatenar buckets amigos.
        """
        found, bucket = self.search(key)
        if not found or not bucket:
            return False
        success, _ = bucket.remove(key)
        if success:
            self.try_merge_buckets(bucket)
        return success

    def splitBucket(self, bucket: Bucket) -> 'Directory':
        """
        Realiza o split de um bucket cheio, criando um novo bucket e redistribuindo as chaves.
        Atualiza as referências do diretório conforme a profundidade local.
        """
        if bucket.localDepth == self.__globalDepth:
            self.double()
        old_rrn = bucket.rrn
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
        bucket.save()
        new_bucket.save()
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

    def try_merge_buckets(self, bucket: Bucket):
        """
        Tenta concatenar o bucket com seu bucket amigo após uma remoção.
        Se possível, move todas as chaves para um bucket, marca o outro como inativo (localDepth = -1),
        atualiza as referências do diretório e reduz a profundidade local.
        """
        if bucket.localDepth <= 1 or bucket.localDepth == -1:
            return  # Não pode concatenar
        
        # Encontra o índice de um dos diretórios que aponta para este bucket
        for idx, ref in enumerate(self.__refs):
            if ref == bucket.rrn:
                bucket_index = idx
                break
        else:
            return  # Não encontrado
        
        # Calcula o índice do bucket amigo (flip do bit mais significativo da profundidade local)
        mask = 1 << (bucket.localDepth - 1)
        buddy_index = bucket_index ^ mask
        buddy_rrn = self.__refs[buddy_index]

        if buddy_rrn == bucket.rrn or buddy_rrn == -1:
            return  # Não há bucket amigo válido
        
        buddy = self._load_bucket(buddy_rrn)
        if buddy.localDepth != bucket.localDepth or buddy.localDepth == -1:
            return  # Só pode juntar se profundidade local igual e ambos ativos
        
        # Soma das chaves
        keys_bucket = [k for k in bucket.keys if k != -1]
        keys_buddy = [k for k in buddy.keys if k != -1]
        if len(keys_bucket) + len(keys_buddy) > TAM_MAX_BUCKET:
            return  # Não cabe
        
        # Junta as chaves no bucket amigo (pode ser qualquer um)
        merged_keys = keys_bucket + keys_buddy

        # Limpa ambos
        bucket.keys = [-1]*TAM_MAX_BUCKET
        bucket.set_key_counter(0)
        buddy.keys = [-1]*TAM_MAX_BUCKET
        buddy.set_key_counter(0)

        # Coloca as chaves no buddy
        for i, k in enumerate(merged_keys):
            buddy.keys[i] = k
        buddy.set_key_counter(len(merged_keys))
        buddy.localDepth -= 1
        buddy.save()

        # Marca bucket como inativo
        bucket.localDepth = -1
        bucket.set_key_counter(0)
        bucket.save()

        # Atualiza referências do diretório
        for i in range(len(self.__refs)):
            if self.__refs[i] == bucket.rrn or self.__refs[i] == buddy.rrn:
                # Se o bit mais significativo da profundidade local agora for 0, aponta para buddy
                if ((i >> buddy.localDepth) & 1) == ((buddy_index >> buddy.localDepth) & 1):
                    self.__refs[i] = buddy.rrn

        # Após merge, pode ser possível reduzir a profundidade global
        self.try_shrink_directory()

    def try_shrink_directory(self):
        """
        Tenta reduzir a profundidade global se todos os buckets ativos tiverem profundidade local < global.
        """
        min_local = min(
            self._load_bucket(ref).localDepth
            for ref in set(self.__refs)
            if ref != -1 and self._load_bucket(ref).localDepth != -1
        )
        
        while self.__globalDepth > 1 and min_local < self.__globalDepth:
            self.__globalDepth -= 1
            self.__refs = self.__refs[: 2 ** self.__globalDepth]
            
            min_local = min(
                self._load_bucket(ref).localDepth
                for ref in set(self.__refs)
                if ref != -1 and self._load_bucket(ref).localDepth != -1
            )

    def save(self) -> 'Directory':
        """
        Salva o estado do diretório no arquivo especificado por DIR_FILENAME.
        Armazena a profundidade global e os RRNs dos buckets.
        """
        with open(DIR_FILENAME, 'wb') as f:
            f.write(struct.pack('i', self.__globalDepth))
            for ref in self.__refs:
                f.write(struct.pack('i', ref))
        return self

    def load(self) -> 'Directory':
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