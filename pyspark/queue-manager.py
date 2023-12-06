### import os
import os

### set paths
root = '/Users/b06/Desktop/yeardream/medi-05/'
path = f'{root}/spark-scala-project/pyspark/queue.txt'
size = len(os.listdir(f'{root}/data/naverplace_meta/'))
idxs = [i for i in range(1, size+1)]

### create a queue txt file
print(path, size, idxs)
with open(path, 'w') as file:
    for idx in idxs:
        file.write(str(idx)+'\n')
print('created a queue text file')